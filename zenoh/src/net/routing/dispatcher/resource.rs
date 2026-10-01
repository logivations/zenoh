//
// Copyright (c) 2023 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//
use std::{
    any::Any,
    borrow::{Borrow, Cow},
    collections::VecDeque,
    convert::TryInto,
    fmt::Debug,
    hash::{Hash, Hasher},
    ops::{Deref, DerefMut},
    sync::{Arc, RwLock, Weak},
};

use zenoh_collections::{IntHashMap, IntHashSet, SingleOrBoxHashSet};
use zenoh_protocol::{
    core::{key_expr::keyexpr, ExprId, Region, WireExpr},
    network::{
        self,
        declare::{self, queryable::ext::QueryableInfoType, Declare, DeclareBody, DeclareKeyExpr},
        interest::InterestId,
        Mapping, RequestId,
    },
};
use zenoh_sync::{get_mut_unchecked, Cache, CacheValueType};

use super::{
    face::FaceState,
    pubsub::SubscriberInfo,
    tables::{TablesData, TablesLock},
};
use crate::net::routing::{
    dispatcher::{
        face::{Face, FaceId},
        region::RegionMap,
        tables::{RoutingExpr, Tables},
    },
    interceptor::{InterceptorTrait, InterceptorsChain},
    RoutingContext,
};

pub(crate) type NodeId = u16;

/// [`NodeId`] value of [`network::ext::NodeIdType::DEFAULT`].
pub(crate) const DEFAULT_NODE_ID: NodeId = network::ext::NodeIdType::<0>::DEFAULT.node_id;

/// Returns `Some(node_id)` if it represents a router region source, `None` if default.
///
/// Useful for tracing instrumentation to omit default/uninteresting [`NodeId`]s from spans.
#[inline]
pub(crate) const fn node_id_as_source(node_id: NodeId) -> Option<NodeId> {
    if node_id != DEFAULT_NODE_ID {
        Some(node_id)
    } else {
        None
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Direction {
    pub(crate) dst_face: Arc<FaceState>,
    pub(crate) wire_expr: WireExpr<'static>,
    pub(crate) node_id: NodeId,
}

#[derive(Clone, Debug)]
pub(crate) struct QueryDirection {
    pub(crate) dir: Direction,
    pub(crate) rid: RequestId,
}

pub(crate) type Route = Vec<Direction>;

#[derive(Clone, Debug)]
pub(crate) struct QueryTargetQabl {
    pub(crate) dir: Direction,
    pub(crate) info: Option<QueryableInfoType>,
    pub(crate) region: Region,
}

impl QueryTargetQabl {
    pub(crate) fn new(
        ctx: &FaceContext,
        expr: &RoutingExpr,
        complete: bool,
        region: &Region,
    ) -> Option<Self> {
        let qabl = ctx.qabl?;
        let wire_expr = expr.get_best_key(ctx.face.id);
        Some(Self {
            dir: Direction {
                dst_face: ctx.face.clone(),
                wire_expr: wire_expr.to_owned(),
                node_id: DEFAULT_NODE_ID,
            },
            info: Some(QueryableInfoType {
                complete: complete && qabl.complete,
                // NOTE: local client faces are nearer than remote client faces
                distance: if ctx.face.is_local { 0 } else { 1 },
            }),
            region: *region,
        })
    }
}

pub(crate) type QueryTargetQablSet = Vec<QueryTargetQabl>;

/// Helper struct to build route, handling face deduplication.
pub(crate) struct RouteBuilder<T> {
    /// The route built.
    route: Vec<T>,
    /// The faces' id already inserted.
    faces: IntHashSet<usize>,
}

impl<T> RouteBuilder<T> {
    /// Creates a new empty builder.
    pub(crate) fn new() -> Self {
        Self {
            route: Vec::new(),
            faces: IntHashSet::new(),
        }
    }

    /// Insert a new direction if it has not been registered for the given face.
    pub(crate) fn insert(&mut self, face_id: FaceId, direction: impl FnOnce() -> T) {
        if self.faces.insert(face_id) {
            self.route.push(direction());
        }
    }

    pub(crate) fn try_insert(&mut self, face_id: usize, direction: impl FnOnce() -> Option<T>) {
        if !self.faces.contains(&face_id) {
            if let Some(direction) = direction() {
                self.faces.insert(face_id);
                self.route.push(direction);
            }
        }
    }

    /// Build the route, consuming the builder.
    pub(crate) fn build(self) -> Vec<T> {
        self.route
    }
}

pub(crate) struct InterceptorCache(Cache<Option<Box<dyn Any + Send + Sync>>>);
pub(crate) type InterceptorCacheValueType = CacheValueType<Option<Box<dyn Any + Send + Sync>>>;

impl InterceptorCache {
    pub(crate) fn new(value: Option<Box<dyn Any + Send + Sync>>, version: usize) -> Self {
        Self(Cache::<Option<Box<dyn Any + Send + Sync>>>::new(
            value, version,
        ))
    }

    pub(crate) fn empty() -> Self {
        InterceptorCache::new(None, 0)
    }

    #[inline]
    fn value(
        &self,
        interceptor: &InterceptorsChain,
        resource: &Resource,
    ) -> Option<InterceptorCacheValueType> {
        self.0
            .value(interceptor.version, || {
                interceptor.compute_keyexpr_cache(resource.keyexpr()?)
            })
            .ok()
    }
}

pub(crate) struct FaceContext {
    pub(crate) face: Arc<FaceState>,
    pub(crate) local_expr_id: Option<ExprId>,
    pub(crate) remote_expr_id: Option<ExprId>,
    pub(crate) subs: Option<SubscriberInfo>,
    pub(crate) qabl: Option<QueryableInfoType>,
    pub(crate) token: bool,
    pub(crate) subscriber_interest_finalized: bool,
    pub(crate) queryable_interest_finalized: bool,
    pub(crate) in_interceptor_cache: InterceptorCache,
    pub(crate) e_interceptor_cache: InterceptorCache,
}

impl FaceContext {
    pub(crate) fn new(face: Arc<FaceState>) -> Self {
        Self {
            face,
            local_expr_id: None,
            remote_expr_id: None,
            subs: None,
            qabl: None,
            token: false,
            subscriber_interest_finalized: false,
            queryable_interest_finalized: false,
            in_interceptor_cache: InterceptorCache::empty(),
            e_interceptor_cache: InterceptorCache::empty(),
        }
    }
}

/// Global version number for route computation.
/// Use 64bit to not care about rollover.
pub type RoutesVersion = u64;

/// Per-hat data/query routes.
///
/// 1. Routes depend on the source region of a message.
///
///   + For instance, a north peer hat N may route a message in its region only if said message
///     arrives from a south-bound face, otherwise N would not route messages within its region.
///     Thus routes depend on the source bound of a message.
///
///   + Given two south peer sub-regions, say S1 and S2, S1 may route a message to peers in its
///     region only said the message originates in S2 and vice-versa. Thus routes not only depend
///     on the source bound of a message but on its source region more generally.
///
/// 2. Routes depend on the source node id for router hats. In a `R1 - R - R2` topology, R would
///    route a message to R1 only if it originates in R2 and vice-versa.
pub(crate) struct Routes<T> {
    /// Mapping from **source** [`Region`] and [`NodeId`] to data/query routes.
    mapping: RegionMap<NodeIdMap<T>>,
    version: u64,
}

pub(crate) type NodeIdMap<T> = Vec<Option<T>>;

impl<T> Default for Routes<T> {
    fn default() -> Self {
        Self {
            mapping: RegionMap::default(),
            version: 0,
        }
    }
}

impl<T> Routes<T> {
    pub(crate) fn clear(&mut self) {
        self.mapping.clear();
    }

    #[inline]
    pub(crate) fn get_route(
        &self,
        version: RoutesVersion,
        region: &Region,
        node_id: NodeId,
    ) -> Option<&T> {
        if version != self.version {
            return None;
        }

        self.mapping
            .get(region)
            .and_then(|rs| rs.get(node_id as usize))
            .and_then(|r| r.as_ref())
    }

    #[inline]
    pub(crate) fn set_route(
        &mut self,
        version: RoutesVersion,
        region: &Region,
        node_id: NodeId,
        route: T,
    ) {
        if self.version != version {
            self.clear();
            self.version = version;
        }

        let aux = |routes: &mut NodeIdMap<T>| {
            routes.resize_with(node_id as usize + 1, || None);
            routes[node_id as usize] = Some(route);
        };

        if let Some(routes) = self.mapping.get_mut(region) {
            aux(routes);
        } else {
            let mut routes = NodeIdMap::default();
            aux(&mut routes);
            self.mapping.insert(*region, routes);
        }
    }
}

pub(crate) fn get_or_set_route<T: Clone>(
    routes: &RwLock<Routes<T>>,
    version: RoutesVersion,
    region: &Region,
    node_id: NodeId,
    compute_route: impl FnOnce() -> T,
) -> T {
    if let Some(route) = routes.read().unwrap().get_route(version, region, node_id) {
        return route.clone();
    }
    let mut routes = routes.write().unwrap();
    // NOTE(regions): we supposedly re-read the routes here because they might've changed, but I'm
    // not sure this is true given that all callers would've acquired `TablesLock::tables`.
    if let Some(route) = routes.get_route(version, region, node_id) {
        return route.clone();
    }
    let route = compute_route();
    routes.set_route(version, region, node_id, route.clone());
    route
}

pub(crate) type DataRoutes = Routes<Arc<Route>>;
pub(crate) type QueryRoutes = Routes<Arc<QueryTargetQablSet>>;

pub(crate) struct ResourceContext {
    pub(crate) matches: Vec<Weak<Resource>>,
    pub(crate) hats: RegionMap<HatResourceContext>,
    pub(crate) data_routes: RwLock<DataRoutes>,
    #[cfg(feature = "stats")]
    pub(crate) stats_keys: zenoh_stats::StatsKeyCache,
}

impl ResourceContext {
    pub(crate) fn new(hat: RegionMap<HatResourceContext>) -> ResourceContext {
        ResourceContext {
            matches: Vec::new(),
            hats: hat,
            data_routes: Default::default(),
            #[cfg(feature = "stats")]
            stats_keys: Default::default(),
        }
    }

    pub(crate) fn disable_data_routes(&mut self) {
        self.data_routes.get_mut().unwrap().clear();
    }
}

pub(crate) struct HatResourceContext {
    /// Map from `Region` to `HatContext`.
    pub(crate) ctx: Box<dyn Any + Send + Sync>,
    pub(crate) data_routes: RwLock<DataRoutes>,
    pub(crate) query_routes: RwLock<QueryRoutes>,
}

impl HatResourceContext {
    pub(crate) fn new(ctx: Box<dyn Any + Send + Sync>) -> Self {
        HatResourceContext {
            ctx,
            data_routes: Default::default(),
            query_routes: Default::default(),
        }
    }

    pub(crate) fn disable_data_routes(&mut self) {
        self.data_routes.get_mut().unwrap().clear();
    }

    pub(crate) fn disable_query_routes(&mut self) {
        self.query_routes.get_mut().unwrap().clear();
    }
}

pub struct Resource {
    pub(crate) parent: Option<Arc<Resource>>,
    pub(crate) expr: String,
    pub(crate) suffix: usize,
    pub(crate) nonwild_prefix: Option<Arc<Resource>>,
    pub(crate) children: SingleOrBoxHashSet<Child>,
    // Immediate wildcard children require the general key-expression traversal.
    wildcard_children: usize,
    pub(crate) ctx: Option<Box<ResourceContext>>,
    pub(crate) face_ctxs: IntHashMap<FaceId, Arc<FaceContext>>,
}

impl PartialEq for Resource {
    fn eq(&self, other: &Self) -> bool {
        self.expr() == other.expr()
    }
}
impl Eq for Resource {}

// NOTE: The `clippy::mutable_key_type` lint takes issue with the fact that `Resource` contains
// interior mutable data. A configuration option is used to assert that the accessed fields are
// not interior mutable in clippy.toml. Thus care should be taken to ensure soundness of this impl
// as Clippy will not warn about its usage in sets/maps.
impl Hash for Resource {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.expr().hash(state);
    }
}

impl Debug for Resource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.expr())
    }
}

#[derive(Clone)]
pub(crate) struct Child(Arc<Resource>);

impl Deref for Child {
    type Target = Arc<Resource>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Child {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl PartialEq for Child {
    fn eq(&self, other: &Self) -> bool {
        self.0.suffix() == other.0.suffix()
    }
}

impl Eq for Child {}

impl Hash for Child {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.suffix().hash(state);
    }
}

impl Borrow<str> for Child {
    fn borrow(&self) -> &str {
        self.0.suffix()
    }
}

impl Resource {
    fn new(parent: &Arc<Resource>, suffix: &str, context: Option<ResourceContext>) -> Resource {
        let nonwild_prefix = match &parent.nonwild_prefix {
            None => {
                if suffix.contains('*') {
                    Some(parent.clone())
                } else {
                    None
                }
            }
            Some(prefix) => Some(prefix.clone()),
        };

        Resource {
            parent: Some(parent.clone()),
            expr: parent.expr.clone() + suffix,
            suffix: parent.expr.len(),
            nonwild_prefix,
            children: SingleOrBoxHashSet::new(),
            wildcard_children: 0,
            ctx: context.map(Box::new),
            face_ctxs: IntHashMap::new(),
        }
    }

    pub fn expr(&self) -> &str {
        &self.expr
    }

    pub fn keyexpr(&self) -> Option<&keyexpr> {
        if self.parent.is_none() {
            None
        } else {
            // SAFETY: non-root resources are valid keyexprs
            unsafe { Some(keyexpr::from_str_unchecked(&self.expr)) }
        }
    }

    pub fn suffix(&self) -> &str {
        &self.expr[self.suffix..]
    }

    #[inline(always)]
    pub(crate) fn context(&self) -> &ResourceContext {
        self.ctx.as_ref().unwrap()
    }

    #[inline(always)]
    pub(crate) fn context_mut(&mut self) -> &mut ResourceContext {
        self.ctx.as_mut().unwrap()
    }

    pub(crate) fn matches(&self, other: &Resource) -> bool {
        // NOTE: we expect matched resources to always have a context; i.e. correspond to a declared entity.
        // For now, this is an invariant worth checking in debug mode, until we are confident it always holds.
        debug_assert!(self.ctx.is_some());

        self.ctx.as_ref().is_some_and(|ctx| {
            ctx.matches
                .iter()
                .any(|m| m.upgrade().is_some_and(|m| &*m == other))
        })
    }

    pub fn nonwild_prefix(res: &Arc<Resource>) -> (Option<Arc<Resource>>, String) {
        match &res.nonwild_prefix {
            None => (Some(res.clone()), "".to_string()),
            Some(nonwild_prefix) => {
                if !nonwild_prefix.expr().is_empty() {
                    (
                        Some(nonwild_prefix.clone()),
                        res.expr[nonwild_prefix.expr.len()..].to_string(),
                    )
                } else {
                    (None, res.expr().to_string())
                }
            }
        }
    }

    pub fn root() -> Arc<Resource> {
        Arc::new(Resource {
            parent: None,
            expr: String::from(""),
            suffix: 0,
            nonwild_prefix: None,
            children: SingleOrBoxHashSet::new(),
            wildcard_children: 0,
            ctx: None,
            face_ctxs: IntHashMap::new(),
        })
    }

    #[tracing::instrument(level = "trace")]
    pub fn clean(res: &mut Arc<Resource>) {
        let mut resclone = res.clone();
        let mutres = get_mut_unchecked(&mut resclone);
        if let Some(ref mut parent) = mutres.parent {
            tracing::trace!(strong_count = Arc::strong_count(res));
            if Arc::strong_count(res) <= 3 && res.children.is_empty() {
                // consider only childless resource held by only one external object (+ 1 strong count for resclone, + 1 strong count for res.parent to a total of 3 )
                tracing::debug!("Unregister resource {}", res.expr());
                if let Some(context) = mutres.ctx.as_mut() {
                    let res_ptr = Arc::as_ptr(res);
                    for match_ in &mut context.matches {
                        let mut match_ = match_.upgrade().unwrap();
                        if !Arc::ptr_eq(&match_, res) {
                            let mutmatch = get_mut_unchecked(&mut match_);
                            if let Some(ctx) = mutmatch.ctx.as_mut() {
                                ctx.matches.retain(|x| {
                                    // Keep the live-match invariant without acquiring
                                    // a temporary strong owner just to compare identity.
                                    assert!(
                                        x.strong_count() != 0,
                                        "expired resource in match list"
                                    );
                                    x.as_ptr() != res_ptr
                                });
                            }
                        }
                    }
                }
                mutres.nonwild_prefix.take();
                {
                    let parent = get_mut_unchecked(parent);
                    if parent.children.remove(res.suffix()) && res.suffix().contains('*') {
                        parent.wildcard_children -= 1;
                    }
                }
                Resource::clean(parent);
            }
        }
    }

    pub fn close(self: &mut Arc<Resource>) {
        let r = get_mut_unchecked(self);
        for mut c in r.children.drain() {
            Self::close(&mut c);
        }
        r.wildcard_children = 0;
        r.parent.take();
        r.nonwild_prefix.take();
        r.ctx.take();
        r.face_ctxs.clear();
    }

    #[cfg(test)]
    pub fn print_tree(from: &Arc<Resource>) -> String {
        let mut result = from.expr().to_string();
        result.push('\n');
        for child in from.children.iter() {
            result.push_str(&Resource::print_tree(child));
        }
        result
    }

    #[tracing::instrument(level = "debug", skip(tables), ret)]
    pub fn make_resource(
        tables: &mut Tables,
        from: &mut Arc<Resource>,
        mut suffix: &str,
    ) -> Arc<Resource> {
        if !suffix.is_empty() && !suffix.starts_with('/') {
            if let Some(parent) = &mut from.parent.clone() {
                return Resource::make_resource(tables, parent, &[from.suffix(), suffix].concat());
            }
        }
        let mut from = from.clone();
        // do not use recursion as the tree may have arbitrary depth
        while let Some((chunk, rest)) = Self::split_first_chunk(suffix) {
            if let Some(child) = get_mut_unchecked(&mut from).children.get(chunk) {
                from = child.0.clone();
            } else {
                let new = Arc::new(Resource::new(&from, chunk, None));
                if rest.is_empty() {
                    tracing::debug!("Register resource {}", new.expr());
                }
                let parent = get_mut_unchecked(&mut from);
                if parent.children.insert(Child(new.clone())) && chunk.contains('*') {
                    parent.wildcard_children += 1;
                }
                from = new;
            };
            suffix = rest;
        }
        let hat = tables
            .hats
            .map_ref(|d| HatResourceContext::new(d.new_resource()));
        Resource::upgrade_resource(&mut from, hat);
        from
    }

    #[inline]
    pub fn get_resource_ref<'a>(
        mut from: &'a Arc<Resource>,
        mut suffix: &str,
    ) -> Option<&'a Arc<Resource>> {
        if !suffix.is_empty() && !suffix.starts_with('/') {
            if let Some(parent) = &from.parent {
                return Resource::get_resource_ref(parent, &[from.suffix(), suffix].concat());
            }
        }
        // do not use recursion as the tree may have arbitrary depth
        while let Some((chunk, rest)) = Self::split_first_chunk(suffix) {
            (from, suffix) = (from.children.get(chunk)?, rest);
        }
        Some(from)
    }

    #[inline]
    pub fn get_resource(from: &Arc<Resource>, suffix: &str) -> Option<Arc<Resource>> {
        Self::get_resource_ref(from, suffix).cloned()
    }

    /// Split the suffix at the next '/' (after leading one), returning None if the suffix is empty.
    ///
    /// Suffix usually starts with '/', so this first slash is kept as part of the split chunk.
    /// The rest will contain the slash of the split.
    /// For example `split_first_chunk("/a/b") == Some(("/a", "/b"))`.
    #[inline(always)]
    fn split_first_chunk(suffix: &str) -> Option<(&str, &str)> {
        if suffix.is_empty() {
            return None;
        }
        // don't count the first char which may be a leading slash to find the next one
        Some(match suffix[1..].find('/') {
            // don't forget to add 1 to the index because of `[1..]` slice above
            Some(idx) => suffix.split_at(idx + 1),
            None => (suffix, ""),
        })
    }

    #[inline]
    pub fn decl_key(res: &Arc<Resource>, face: &mut Arc<FaceState>) -> WireExpr<'static> {
        if face.is_local {
            return res.expr().to_string().into();
        }

        let (nonwild_prefix, wildsuffix) = Resource::nonwild_prefix(res);
        match nonwild_prefix {
            Some(mut nonwild_prefix) => {
                if let Some(ctx) = get_mut_unchecked(&mut nonwild_prefix)
                    .face_ctxs
                    .get(&face.id)
                {
                    if let Some(expr_id) = ctx.remote_expr_id {
                        return WireExpr {
                            scope: expr_id,
                            suffix: wildsuffix.into(),
                            mapping: Mapping::Receiver,
                        };
                    }
                    if let Some(expr_id) = ctx.local_expr_id {
                        return WireExpr {
                            scope: expr_id,
                            suffix: wildsuffix.into(),
                            mapping: Mapping::Sender,
                        };
                    }
                }
                if face.region.bound().is_north()
                    || face.remote_key_interests.values().any(|res| {
                        res.as_ref()
                            .map(|res| res.matches(&nonwild_prefix))
                            .unwrap_or(true)
                    })
                {
                    let ctx = get_mut_unchecked(&mut nonwild_prefix)
                        .face_ctxs
                        .entry(face.id)
                        .or_insert_with(|| Arc::new(FaceContext::new(face.clone())));
                    let expr_id = face.get_next_local_id();
                    get_mut_unchecked(ctx).local_expr_id = Some(expr_id);
                    get_mut_unchecked(face)
                        .local_mappings
                        .insert(expr_id, nonwild_prefix.clone());
                    face.primitives.send_declare(RoutingContext::with_expr(
                        &mut Declare {
                            interest_id: None,
                            ext_qos: declare::ext::QoSType::DECLARE,
                            ext_tstamp: None,
                            ext_nodeid: declare::ext::NodeIdType::DEFAULT,
                            body: DeclareBody::DeclareKeyExpr(DeclareKeyExpr {
                                id: expr_id,
                                wire_expr: nonwild_prefix.expr().to_string().into(),
                            }),
                        },
                        nonwild_prefix.expr().to_string(),
                    ));
                    face.update_interceptors_caches(&mut nonwild_prefix);
                    WireExpr {
                        scope: expr_id,
                        suffix: wildsuffix.into(),
                        mapping: Mapping::Sender,
                    }
                } else {
                    res.expr().to_string().into()
                }
            }
            None => wildsuffix.into(),
        }
    }

    /// Return the best locally/remotely declared keyexpr, i.e. with the smallest suffix, matching
    /// the given suffix and session id.
    ///
    /// The goal is to save bandwidth by using the shortest keyexpr on the wire. It works by
    /// recursively walk through the children tree, looking for an already declared keyexpr for the
    /// session.
    /// If none is found, and if the tested resource itself doesn't have a declared keyexpr,
    /// then the parent tree is walked through. If there is still no declared keyexpr, the whole
    /// prefix+suffix string is used.
    pub fn get_best_key<'a>(&self, suffix: &'a str, sid: usize) -> WireExpr<'a> {
        /// Retrieve a declared keyexpr, either local or remote.
        fn get_wire_expr<'a>(
            prefix: &Resource,
            suffix: impl FnOnce() -> Cow<'a, str>,
            sid: usize,
        ) -> Option<WireExpr<'a>> {
            let ctx = prefix.face_ctxs.get(&sid)?;
            let (scope, mapping) = match (ctx.remote_expr_id, ctx.local_expr_id) {
                (Some(expr_id), _) => (expr_id, Mapping::Receiver),
                (_, Some(expr_id)) => (expr_id, Mapping::Sender),
                _ => return None,
            };
            Some(WireExpr {
                scope,
                suffix: suffix(),
                mapping,
            })
        }
        /// Walk through the children tree, looking for a declared keyexpr.
        fn get_best_child_key<'a>(
            mut prefix: &Resource,
            suffix: &'a str,
            sid: usize,
        ) -> Option<WireExpr<'a>> {
            let mut suffix_rest = suffix;
            // do not use recursion as the tree may have arbitrary depth
            // first we get the closest matching child
            while let Some((chunk, rest)) = Resource::split_first_chunk(suffix_rest) {
                match prefix.children.get(chunk) {
                    Some(child) => prefix = child,
                    None => break,
                }
                suffix_rest = rest;
            }
            // then we go backward checking the child and its parents
            while suffix_rest != suffix {
                if let Some(wire_expr) = get_wire_expr(prefix, || suffix_rest.into(), sid) {
                    return Some(wire_expr);
                }
                suffix_rest = &suffix[suffix.len() - suffix_rest.len() - prefix.suffix().len()..];
                prefix = prefix.parent.as_ref().unwrap();
            }
            None
        }
        /// Walk through the parent tree, looking for a declared keyexpr.
        fn get_best_parent_key<'a>(
            prefix: &Resource,
            suffix: &'a str,
            sid: usize,
            mut parent: &Resource,
        ) -> Option<WireExpr<'a>> {
            // do not use recursion as the tree may have arbitrary depth
            loop {
                let parent_suffix = || [&prefix.expr[parent.expr.len()..], suffix].concat().into();
                if let Some(wire_expr) = get_wire_expr(parent, parent_suffix, sid) {
                    return Some(wire_expr);
                }
                {
                    let p = parent.parent.as_ref()?;
                    parent = p
                }
            }
        }
        get_best_child_key(self, suffix, sid)
            .or_else(|| get_wire_expr(self, || suffix.into(), sid))
            .or_else(|| get_best_parent_key(self, suffix, sid, self.parent.as_ref()?))
            .unwrap_or_else(|| [&self.expr, suffix].concat().into())
    }

    pub fn get_matches(tables: &TablesData, key_expr: &keyexpr) -> Vec<Weak<Resource>> {
        // A literal chunk only intersects an identical literal chunk. When no
        // child contains a wildcard, use the existing child hash table instead
        // of visiting every unrelated sibling. Preserve the original traversal
        // for wildcard children and the slash-only intermediate resource.
        #[inline(always)]
        fn enqueue_children<'a>(
            key_expr: &'a keyexpr,
            from: &'a Arc<Resource>,
            nodes: &mut VecDeque<(&'a keyexpr, &'a Arc<Resource>)>,
        ) {
            let children = match &from.children {
                SingleOrBoxHashSet::Empty => return,
                SingleOrBoxHashSet::Single(child) => {
                    nodes.push_back((key_expr, child));
                    return;
                }
                SingleOrBoxHashSet::Set(children) => children,
            };
            if children.len() > 1 && from.wildcard_children == 0 && children.get("/").is_none() {
                let chunk = key_expr
                    .split_once('/')
                    .map_or(key_expr.as_str(), |(c, _)| c);
                if !chunk.contains('*') {
                    if let Some(child) = children.get(chunk) {
                        nodes.push_back((key_expr, child));
                    }
                    // Resource suffixes may include the separator; accept both
                    // representations just as the general traversal does.
                    let mut suffix = String::with_capacity(chunk.len() + 1);
                    suffix.push('/');
                    suffix.push_str(chunk);
                    if let Some(child) = children.get(suffix.as_str()) {
                        nodes.push_back((key_expr, child));
                    }
                    return;
                }
            }
            for child in children.iter() {
                nodes.push_back((key_expr, child));
            }
        }
        pub fn visit_nodes<T>(node: T, mut visit: impl FnMut(T, &mut VecDeque<T>)) {
            let mut nodes = VecDeque::from([node]);
            while let Some(node) = nodes.pop_front() {
                visit(node, &mut nodes);
            }
        }
        fn get_matches_from(
            key_expr: &keyexpr,
            from: &Arc<Resource>,
            matches: &mut Vec<Weak<Resource>>,
        ) {
            #[cfg(test)]
            let mut visited = 0;
            visit_nodes((key_expr, from), |(key_expr, from), nodes| {
                #[cfg(test)]
                {
                    visited += 1;
                }
                if from.parent.is_none() || from.suffix() == "/" {
                    enqueue_children(key_expr, from, nodes);
                    return;
                }
                let suffix: &keyexpr = from
                    .suffix()
                    .strip_prefix('/')
                    .unwrap_or(from.suffix())
                    .try_into()
                    .unwrap();
                let (ke_chunk, ke_rest) = match key_expr.split_once('/') {
                    // SAFETY: chunks of keyexpr are valid keyexprs
                    Some((chunk, rest)) => unsafe {
                        (
                            keyexpr::from_str_unchecked(chunk),
                            Some(keyexpr::from_str_unchecked(rest)),
                        )
                    },
                    None => (key_expr, None),
                };
                let ke_chunk_intersects_suffix = ke_chunk.intersects(suffix);
                let ke_chunk_is_wild = ke_chunk.as_bytes() == b"**";
                let suffix_is_wild = suffix.as_bytes() == b"**";
                match ke_rest {
                    None => {
                        if ke_chunk_intersects_suffix {
                            if from.ctx.is_some() {
                                matches.push(Arc::downgrade(from));
                            }
                            if let Some(child) =
                                from.children.get("/**").or_else(|| from.children.get("**"))
                            {
                                if child.ctx.is_some() {
                                    matches.push(Arc::downgrade(child))
                                }
                            }
                        }
                        if (ke_chunk_is_wild && ke_chunk_intersects_suffix) || suffix_is_wild {
                            enqueue_children(key_expr, from, nodes);
                        }
                    }
                    Some(rest) => {
                        if ke_chunk_intersects_suffix
                            && rest.as_bytes() == b"**"
                            && from.ctx.is_some()
                        {
                            matches.push(Arc::downgrade(from));
                        }
                        if (ke_chunk_is_wild && ke_chunk_intersects_suffix) || suffix_is_wild {
                            enqueue_children(key_expr, from, nodes);
                        } else if ke_chunk_intersects_suffix {
                            enqueue_children(rest, from, nodes);
                        }
                        if (suffix_is_wild && ke_chunk_intersects_suffix) || ke_chunk_is_wild {
                            nodes.push_back((rest, from));
                        }
                    }
                };
            });
            #[cfg(test)]
            literal_child_tests::LAST_VISITS.set(visited);
        }
        let mut matches = Vec::new();
        get_matches_from(key_expr, &tables.root_res, &mut matches);
        matches.sort_unstable_by_key(Weak::as_ptr);
        matches.dedup_by_key(|res| Weak::as_ptr(res));
        matches
    }

    pub fn match_resource(
        _tables: &TablesData,
        res: &mut Arc<Resource>,
        matches: Vec<Weak<Resource>>,
    ) {
        if res.ctx.is_some() {
            for match_ in &matches {
                let mut match_ = match_.upgrade().unwrap();
                get_mut_unchecked(&mut match_)
                    .context_mut()
                    .matches
                    .push(Arc::downgrade(res));
            }
            get_mut_unchecked(res).context_mut().matches = matches;
        } else {
            tracing::error!("Call match_resource() on context less res {}", res.expr());
        }
    }

    pub fn upgrade_resource(res: &mut Arc<Resource>, hat: RegionMap<HatResourceContext>) {
        if res.ctx.is_none() {
            get_mut_unchecked(res).ctx = Some(Box::new(ResourceContext::new(hat)));
        }
    }

    pub(crate) fn get_ingress_cache(
        &self,
        face: &Face,
        interceptor: &InterceptorsChain,
    ) -> Option<InterceptorCacheValueType> {
        self.face_ctxs
            .get(&face.state.id)
            .and_then(|ctx| ctx.in_interceptor_cache.value(interceptor, self))
    }

    pub(crate) fn get_egress_cache(
        &self,
        face: &Face,
        interceptor: &InterceptorsChain,
    ) -> Option<InterceptorCacheValueType> {
        self.face_ctxs
            .get(&face.state.id)
            .and_then(|ctx| ctx.e_interceptor_cache.value(interceptor, self))
    }
}

pub(crate) fn register_expr(
    tables: &TablesLock,
    face: &mut Arc<FaceState>,
    expr_id: ExprId,
    expr: &WireExpr,
) {
    let rtables = zread!(tables.tables);
    match rtables
        .data
        .get_mapping(face, &expr.scope, expr.mapping)
        .cloned()
    {
        Some(mut prefix) => match face.remote_mappings.get(&expr_id) {
            Some(res) => {
                let mut fullexpr = prefix.expr().to_string();
                fullexpr.push_str(expr.suffix.as_ref());
                if res.expr() != fullexpr {
                    tracing::error!(
                        "{} Resource {} remapped. Remapping unsupported!",
                        face,
                        expr_id
                    );
                }
            }
            None => {
                let res = Resource::get_resource(&prefix, &expr.suffix);
                let (mut res, mut wtables) = if res
                    .as_ref()
                    .map(|r| r.ctx.is_some())
                    .unwrap_or(false)
                {
                    drop(rtables);
                    let wtables = zwrite!(tables.tables);
                    (res.unwrap(), wtables)
                } else {
                    let mut fullexpr = prefix.expr().to_string();
                    fullexpr.push_str(expr.suffix.as_ref());
                    let mut matches = keyexpr::new(fullexpr.as_str())
                        .map(|ke| Resource::get_matches(&rtables.data, ke))
                        .unwrap_or_default();
                    drop(rtables);
                    let mut wtables = zwrite!(tables.tables);
                    let mut res =
                        Resource::make_resource(&mut wtables, &mut prefix, expr.suffix.as_ref());
                    matches.push(Arc::downgrade(&res));
                    Resource::match_resource(&wtables.data, &mut res, matches);
                    (res, wtables)
                };
                let ctx = get_mut_unchecked(&mut res)
                    .face_ctxs
                    .entry(face.id)
                    .or_insert_with(|| Arc::new(FaceContext::new(face.clone())));

                get_mut_unchecked(ctx).remote_expr_id = Some(expr_id);

                get_mut_unchecked(face)
                    .remote_mappings
                    .insert(expr_id, res.clone());

                let tables = &mut *wtables;
                let hats = &mut tables.hats;
                let region = face.region;

                hats[region].disable_data_routes(&mut res);
                hats[region].disable_query_routes(&mut res);

                face.update_interceptors_caches(&mut res);
                drop(wtables);
            }
        },
        None => tracing::error!(
            "{} Declare resource with unknown scope {}!",
            face,
            expr.scope
        ),
    }
}

pub(crate) fn unregister_expr(tables: &TablesLock, face: &mut Arc<FaceState>, expr_id: ExprId) {
    let mut wtables = zwrite!(tables.tables);

    let tables = &mut *wtables;
    let hats = &mut tables.hats;
    let region = face.region;

    match get_mut_unchecked(face).remote_mappings.remove(&expr_id) {
        Some(mut res) => {
            if let Some(ctx) = get_mut_unchecked(&mut res).face_ctxs.get_mut(&face.id) {
                get_mut_unchecked(ctx).remote_expr_id = None;
            }
            hats[region].disable_data_routes(&mut res);
            hats[region].disable_query_routes(&mut res);
            face.update_interceptors_caches(&mut res);
            Resource::clean(&mut res);
        }
        None => tracing::error!("{} Undeclare unknown resource!", face),
    }

    drop(wtables);
}

pub(crate) fn register_expr_interest(
    tables: &TablesLock,
    face: &mut Arc<FaceState>,
    id: InterestId,
    expr: Option<&WireExpr>,
) {
    if let Some(expr) = expr {
        let rtables = zread!(tables.tables);
        match rtables
            .data
            .get_mapping(face, &expr.scope, expr.mapping)
            .cloned()
        {
            Some(mut prefix) => {
                let res = Resource::get_resource(&prefix, &expr.suffix);
                let (res, wtables) = if res.as_ref().map(|r| r.ctx.is_some()).unwrap_or(false) {
                    drop(rtables);
                    let wtables = zwrite!(tables.tables);
                    (res.unwrap(), wtables)
                } else {
                    let mut fullexpr = prefix.expr().to_string();
                    fullexpr.push_str(expr.suffix.as_ref());
                    let mut matches = keyexpr::new(fullexpr.as_str())
                        .map(|ke| Resource::get_matches(&rtables.data, ke))
                        .unwrap_or_default();
                    drop(rtables);
                    let mut wtables = zwrite!(tables.tables);
                    let mut res =
                        Resource::make_resource(&mut wtables, &mut prefix, expr.suffix.as_ref());
                    matches.push(Arc::downgrade(&res));
                    Resource::match_resource(&wtables.data, &mut res, matches);
                    (res, wtables)
                };
                get_mut_unchecked(face)
                    .remote_key_interests
                    .insert(id, Some(res));
                drop(wtables);
            }
            None => tracing::error!(
                "{} Declare keyexpr interest with unknown scope {}!",
                face,
                expr.scope,
            ),
        }
    } else {
        let wtables = zwrite!(tables.tables);
        get_mut_unchecked(face)
            .remote_key_interests
            .insert(id, None);
        drop(wtables);
    }
}

#[cfg(test)]
mod literal_child_tests {
    use std::collections::BTreeSet;

    use zenoh_protocol::core::WhatAmI;

    use super::*;
    use crate::net::{primitives::DummyPrimitives, routing::gateway::GatewayBuilder};

    std::thread_local! {
        pub(super) static LAST_VISITS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    }

    fn new_router() -> crate::net::routing::gateway::Gateway {
        let mut config = zenoh_config::Config::default().expanded();
        config.set_mode(Some(WhatAmI::Client)).unwrap();
        GatewayBuilder::new(&config)
            .subregions(vec![Region::Local])
            .build()
            .unwrap()
    }

    fn names(matches: Vec<Weak<Resource>>) -> BTreeSet<String> {
        matches
            .into_iter()
            .map(|resource| resource.upgrade().unwrap().expr().to_owned())
            .collect()
    }

    fn check_child_counts(resource: &Arc<Resource>) {
        assert_eq!(
            resource.wildcard_children,
            resource
                .children
                .iter()
                .filter(|child| child.suffix().contains('*'))
                .count(),
            "wildcard child count at {}",
            resource.expr(),
        );
        for child in resource.children.iter() {
            check_child_counts(child);
        }
    }

    // Walk the entire tree independently of get_matches/pruning. Validate full
    // expressions because wire mappings may end at a slash-only intermediate.
    fn intersecting_resources(root: &Arc<Resource>, query: &keyexpr) -> Vec<*const Resource> {
        let mut expected = Vec::new();
        let mut pending = vec![root];
        while let Some(resource) = pending.pop() {
            if resource.ctx.is_some()
                && keyexpr::new(resource.expr()).is_ok_and(|key| query.intersects(key))
            {
                expected.push(Arc::as_ptr(resource));
            }
            pending.extend(resource.children.iter().map(|child| &child.0));
        }
        expected.sort_unstable();
        expected
    }

    fn assert_query_matches(tables: &TablesData, query: &keyexpr) -> BTreeSet<String> {
        let actual = Resource::get_matches(tables, query);
        assert_eq!(
            actual.iter().map(Weak::as_ptr).collect::<Vec<_>>(),
            intersecting_resources(&tables.root_res, query),
            "keyexpr::intersects resource identities differ for {query}",
        );
        names(actual)
    }

    fn assert_queries_match_intersection(tables: &TablesLock, queries: &[String]) {
        let tables = zread!(tables.tables);
        check_child_counts(&tables.data.root_res);
        for query in queries {
            assert_query_matches(&tables.data, keyexpr::new(query).unwrap());
        }
        let mut pending = vec![&tables.data.root_res];
        while let Some(resource) = pending.pop() {
            if let Some(ctx) = &resource.ctx {
                if let Ok(key) = keyexpr::new(resource.expr()) {
                    let mut actual = ctx
                        .matches
                        .iter()
                        .map(|m| {
                            assert!(m.strong_count() != 0);
                            m.as_ptr()
                        })
                        .collect::<Vec<_>>();
                    actual.sort_unstable();
                    assert_eq!(
                        actual,
                        intersecting_resources(&tables.data.root_res, key),
                        "cached match identities differ for {key}",
                    );
                }
            }
            pending.extend(resource.children.iter().map(|child| &child.0));
        }
    }

    // Includes the upstream matching corpus, namespace prefixes, verbatim
    // chunks, non-declared queries, and combinations at different tree levels.
    fn corpus() -> Vec<String> {
        let mut keys = [
            "**",
            "a",
            "a/b",
            "*",
            "a/*",
            "a/b$*",
            "abc",
            "xx",
            "ab$*",
            "abcd",
            "ab$*d",
            "ab",
            "ab/*",
            "a/*/c/*/e",
            "a/b/c/d/e",
            "a/$*b/c/$*d/e",
            "a/xb/c/xd/e",
            "a/c/e",
            "a/b/c/d/x/e",
            "ab$*cd",
            "abxxcxxd",
            "abxxcxxcd",
            "abxxcxxcdx",
            "a/b/c",
            "ab/**",
            "**/xyz",
            "a/b/xyz/d/e/f/xyz",
            "**/xyz$*xyz",
            "a/**/c/**/e",
            "a/b/b/b/c/d/d/d/e",
            "a/**/c/*/e/*",
            "a/b/b/b/c/d/d/c/d/e/f",
            "x/abc",
            "x/*",
            "x/abc$*",
            "x/$*abc",
            "x/a$*",
            "x/a$*de",
            "x/abc$*de",
            "x/a$*d$*e",
            "x/a$*e",
            "x/a$*c$*e",
            "x/ade",
            "x/c$*",
            "x/$*d",
            "x/$*e",
            "@a",
            "**/@a",
            "@a/b",
            "ns/a/b",
            "ns/**",
            "ns/@private/x",
            "ns/@private/**",
            "ns/*/x",
            "ns/**/@private/**",
            "@/z/@ros2_lv/SC/service",
            "@/z/@ros2_lv/**",
            "@a/**",
            "namespace/stress/base_0",
            "namespace/stress/base_1",
            "namespace/stress/base_$*",
        ]
        .map(str::to_owned)
        .to_vec();
        for first in ["alpha", "beta", "@hidden", "*", "**", "pre$*post"] {
            for second in ["alpha", "beta", "@hidden", "*", "**", "pre$*post"] {
                let key = format!("generated/{first}/{second}");
                if keyexpr::new(&key).is_ok() {
                    keys.push(key);
                }
            }
        }
        keys.sort();
        keys.dedup();
        for key in &keys {
            keyexpr::new(key).unwrap();
        }
        keys
    }

    #[test]
    fn literal_child_pruning_matches_intersection_across_insert_and_remove() {
        let router = new_router();
        let tables = router.tables.clone();
        let mut face = router
            .new_session(Arc::new(DummyPrimitives {}))
            .state
            .clone();
        let keys = corpus();
        let mut queries = keys.clone();
        queries.extend(
            [
                "missing/path",
                "namespace/stress/base_999",
                "ns/@private/missing",
                "generated/pre-middle-post/alpha",
                "a/b/c/d",
                "other/**",
            ]
            .map(str::to_owned),
        );
        assert_queries_match_intersection(&tables, &queries);
        for (index, key) in keys.iter().enumerate().step_by(2) {
            register_expr(
                &tables,
                &mut face,
                (index + 1).try_into().unwrap(),
                &key.as_str().into(),
            );
        }
        assert_queries_match_intersection(&tables, &queries);
        for (index, key) in keys.iter().enumerate().skip(1).step_by(2) {
            register_expr(
                &tables,
                &mut face,
                (index + 1).try_into().unwrap(),
                &key.as_str().into(),
            );
        }
        assert_queries_match_intersection(&tables, &queries);
        for index in (0..keys.len()).step_by(3) {
            unregister_expr(&tables, &mut face, (index + 1).try_into().unwrap());
        }
        assert_queries_match_intersection(&tables, &queries);
        for index in (0..keys.len()).step_by(3).rev() {
            register_expr(
                &tables,
                &mut face,
                (index + 1).try_into().unwrap(),
                &keys[index].as_str().into(),
            );
        }
        assert_queries_match_intersection(&tables, &queries);
        for index in (0..keys.len()).rev() {
            unregister_expr(&tables, &mut face, (index + 1).try_into().unwrap());
        }
        assert_queries_match_intersection(&tables, &queries);
        assert!(zread!(tables.tables).data.root_res.children.is_empty());
    }

    #[test]
    fn clean_rejects_expired_reverse_match() {
        let router = new_router();
        let tables = router.tables.clone();
        let mut face = router
            .new_session(Arc::new(DummyPrimitives {}))
            .state
            .clone();
        register_expr(&tables, &mut face, 1, &"kept/**".into());
        register_expr(&tables, &mut face, 2, &"kept/leaf".into());
        {
            let tables = zwrite!(tables.tables);
            let mut wildcard = Resource::get_resource(&tables.data.root_res, "kept/**").unwrap();
            let expired = {
                let resource = Resource::root();
                Arc::downgrade(&resource)
            };
            assert!(expired.upgrade().is_none());
            get_mut_unchecked(&mut wildcard)
                .context_mut()
                .matches
                .push(expired);
        }
        // The leaf's own list contains only live matches. The expired entry is
        // encountered specifically while removing it from the wildcard's list.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            unregister_expr(&tables, &mut face, 2);
        }));
        let mut tables = tables.tables.write().unwrap_or_else(|e| e.into_inner());
        tables.data.root_res.close();
        assert!(result.is_err(), "cleanup accepted an expired reverse match");
    }

    #[test]
    fn literal_child_pruning_visits_do_not_grow_with_unrelated_siblings() {
        for siblings in [1_u16, 64, 1024] {
            let router = new_router();
            let tables = router.tables.clone();
            let mut face = router
                .new_session(Arc::new(DummyPrimitives {}))
                .state
                .clone();
            for index in 0..siblings {
                let key = format!("root/app/service_{index}");
                register_expr(&tables, &mut face, index + 1, &key.into());
            }
            let query = keyexpr::new("root/app/service_0").unwrap();
            {
                let tables = zread!(tables.tables);
                let actual = assert_query_matches(&tables.data, query);
                assert_eq!(actual, BTreeSet::from(["root/app/service_0".to_owned()]));
                assert_eq!(
                    LAST_VISITS.get(),
                    4,
                    "literal lookup should follow only root/app/service_0"
                );
            }
            // Wildcards must use the general algorithm, and removing the last
            // wildcard must restore literal lookup rather than retaining a flag.
            register_expr(
                &tables,
                &mut face,
                siblings + 1,
                &"root/app/service_$*".into(),
            );
            {
                let tables = zread!(tables.tables);
                check_child_counts(&tables.data.root_res);
                let actual = assert_query_matches(&tables.data, query);
                assert_eq!(actual.len(), 2);
                assert_eq!(LAST_VISITS.get(), u64::from(siblings) + 4);
            }
            unregister_expr(&tables, &mut face, siblings + 1);
            {
                let tables = zread!(tables.tables);
                check_child_counts(&tables.data.root_res);
                assert_query_matches(&tables.data, query);
                assert_eq!(LAST_VISITS.get(), 4);
            }
            for index in (0..siblings).rev() {
                unregister_expr(&tables, &mut face, index + 1);
            }
            check_child_counts(&zread!(tables.tables).data.root_res);
            assert!(zread!(tables.tables).data.root_res.children.is_empty());
        }
    }

    #[test]
    fn literal_child_pruning_close_clears_wildcard_counts() {
        let router = new_router();
        let tables = router.tables.clone();
        let mut face = router
            .new_session(Arc::new(DummyPrimitives {}))
            .state
            .clone();
        for (index, key) in ["a/**/b", "a/*/c", "ns/@verbatim/x", "ns/literal/x"]
            .iter()
            .enumerate()
        {
            register_expr(
                &tables,
                &mut face,
                (index + 1).try_into().unwrap(),
                &(*key).into(),
            );
        }
        let tables = zwrite!(tables.tables);
        let mut root = tables.data.root_res.clone();
        let mut retained = Vec::new();
        let mut pending = vec![root.clone()];
        while let Some(resource) = pending.pop() {
            pending.extend(resource.children.iter().map(|child| child.0.clone()));
            retained.push(resource);
        }
        Resource::close(&mut root);
        for resource in retained {
            assert_eq!(resource.wildcard_children, 0);
            assert_eq!(resource.children.len(), 0);
        }
    }

    #[test]
    fn literal_child_pruning_preserves_mapped_and_partial_prefixes() {
        let router = new_router();
        let tables = router.tables.clone();
        let mut face = router
            .new_session(Arc::new(DummyPrimitives {}))
            .state
            .clone();
        // Wire resource prefixes can end at a separator or inside a chunk.
        register_expr(&tables, &mut face, 1, &"mapped/".into());
        register_expr(
            &tables,
            &mut face,
            2,
            &WireExpr::from(1).with_suffix("topic"),
        );
        register_expr(&tables, &mut face, 3, &"mapped/ser".into());
        register_expr(
            &tables,
            &mut face,
            4,
            &WireExpr::from(3).with_suffix("vice/request"),
        );
        register_expr(
            &tables,
            &mut face,
            5,
            &WireExpr::from(4).with_suffix("/@private/x"),
        );
        register_expr(
            &tables,
            &mut face,
            6,
            &WireExpr::from(1).with_suffix("service/**"),
        );
        let queries = [
            "mapped/topic",
            "mapped/*",
            "mapped/**",
            "mapped/service/request",
            "mapped/service/**",
            "mapped/service/request/@private/x",
            "mapped/service/request/**/@private/*",
            "**/@private/x",
            "mapped/missing",
        ];
        for slash_prefix_present in [true, false] {
            if !slash_prefix_present {
                unregister_expr(&tables, &mut face, 1);
            }
            let tables = zread!(tables.tables);
            check_child_counts(&tables.data.root_res);
            for query in queries {
                assert_query_matches(&tables.data, keyexpr::new(query).unwrap());
            }
            let parent = Resource::get_resource_ref(&tables.data.root_res, "mapped").unwrap();
            assert_eq!(parent.children.get("/").is_some(), slash_prefix_present);
            let request =
                Resource::get_resource_ref(&tables.data.root_res, "mapped/service/request")
                    .unwrap();
            let leaf = Resource::get_resource_ref(request, "/@private/x").unwrap();
            assert_eq!(leaf.expr(), "mapped/service/request/@private/x");
            assert!(leaf.ctx.is_some());
            let topic_matches =
                assert_query_matches(&tables.data, keyexpr::new("mapped/topic").unwrap());
            assert_eq!(topic_matches, BTreeSet::from(["mapped/topic".to_owned()]));
        }
        for id in (2..=6).rev() {
            unregister_expr(&tables, &mut face, id);
        }
        check_child_counts(&zread!(tables.tables).data.root_res);
        assert!(zread!(tables.tables).data.root_res.children.is_empty());
    }
}
