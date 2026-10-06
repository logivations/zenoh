use std::{
    any::Any,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use zenoh_core::ztimeout;
use zenoh_protocol::{
    core::{Reliability, WireExpr, ZenohIdProto},
    network::{Declare, Interest, Mapping, Push, Request, Response, ResponseFinal},
};

use crate::{
    api::{
        queryable::{Query, QueryInner, ReplyPrimitives},
        sample::QoS,
    },
    net::primitives::Primitives,
};

const TIMEOUT: Duration = Duration::from_secs(60);

struct ReplyTestPrimitives {
    wire_expr: Arc<Mutex<Option<WireExpr<'static>>>>,
    finals: AtomicUsize,
    discarded: AtomicUsize,
}

impl ReplyTestPrimitives {
    fn new() -> Self {
        ReplyTestPrimitives {
            wire_expr: Arc::new(Mutex::new(None)),
            finals: AtomicUsize::new(0),
            discarded: AtomicUsize::new(0),
        }
    }

    fn wire_expr(&self) -> Option<WireExpr<'_>> {
        self.wire_expr.lock().unwrap().clone()
    }
}

impl Primitives for ReplyTestPrimitives {
    fn send_interest(&self, _msg: &mut Interest) {}

    fn send_declare(&self, _msg: &mut Declare) {}

    fn send_push(&self, _msg: &mut Push, _reliability: Reliability) {}

    fn send_push_consume(&self, _msg: &mut Push, _reliability: Reliability, _consume: bool) {}

    fn send_request(&self, _msg: &mut Request) {}

    fn send_response(&self, msg: &mut Response) {
        let _ = self.wire_expr.lock().unwrap().insert(msg.wire_expr.clone());
    }

    fn send_response_final(&self, _msg: &mut ResponseFinal) {
        self.finals.fetch_add(1, Ordering::Relaxed);
    }

    fn discard_query(&self, _qid: zenoh_protocol::network::RequestId) {
        self.discarded.fetch_add(1, Ordering::Relaxed);
    }

    fn send_close(&self) {}

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[test]
#[cfg(feature = "internal")]
fn discard_suppresses_final_including_the_dispatcher_clone() {
    fn query(primitives: Arc<ReplyTestPrimitives>) -> Query {
        let mut query = Query::empty();
        Arc::get_mut(&mut query.inner).unwrap().primitives =
            ReplyPrimitives::new_remote(None, primitives);
        query
    }
    let primitives = Arc::new(ReplyTestPrimitives::new());
    query(primitives.clone()).discard();
    assert_eq!(primitives.discarded.load(Ordering::Relaxed), 1);
    assert_eq!(primitives.finals.load(Ordering::Relaxed), 0);

    let normal = query(primitives.clone());
    normal.clone().discard();
    assert_eq!(primitives.discarded.load(Ordering::Relaxed), 1);
    drop(normal);
    assert_eq!(primitives.discarded.load(Ordering::Relaxed), 2);
    assert_eq!(primitives.finals.load(Ordering::Relaxed), 0);

    let first = query(primitives.clone());
    let second = first.clone();
    let other = std::thread::spawn(move || first.discard());
    second.discard();
    other.join().unwrap();
    assert_eq!(primitives.discarded.load(Ordering::Relaxed), 3);
    assert_eq!(primitives.finals.load(Ordering::Relaxed), 0);
    drop(query(primitives.clone()));
    assert_eq!(primitives.finals.load(Ordering::Relaxed), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_reply_preserves_optimized_ke() {
    use crate::Config;

    let session = ztimeout!(crate::open(Config::default())).unwrap();

    let primitives = Arc::new(ReplyTestPrimitives::new());

    let query_inner = QueryInner {
        discarded: false.into(),
        key_expr: "test/**".try_into().unwrap(),
        parameters: "".into(),
        qid: 1,
        zid: ZenohIdProto::default(),
        qos: QoS::default(),
        #[cfg(feature = "unstable")]
        source_info: None,
        primitives: ReplyPrimitives::new_remote(Some(session.downgrade()), primitives.clone()),
        #[cfg(feature = "unstable")]
        runtime: None,
        #[cfg(feature = "unstable")]
        query_ts_stack: None,
    };
    let query = Query {
        inner: Arc::new(query_inner),
        eid: 1,
        value: None,
        attachment: None,
    };

    let ke = "test/reply_declared_ke";
    let declared_ke = ztimeout!(session.declare_keyexpr(ke)).unwrap();
    ztimeout!(query.reply(declared_ke, "payload")).unwrap();

    let mut we = primitives.wire_expr().unwrap();
    assert!(we.suffix.is_empty());
    assert!(we.scope != 0);
    assert!(we.mapping == Mapping::Sender);

    ztimeout!(query.reply(ke, "payload")).unwrap();
    we = primitives.wire_expr().unwrap();
    assert_eq!(&we.suffix, &ke);
    assert!(we.scope == 0);
}

#[cfg(all(feature = "internal", feature = "transport_tcp"))]
#[test_case::test_case(false; "plain")]
#[test_case::test_case(true; "namespace")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discard_releases_routing_state_and_cancels_timeout(namespace: bool) {
    use zenoh_core::zread;

    let mut config = crate::Config::default();
    config
        .set_mode(Some(zenoh_config::WhatAmI::Router))
        .unwrap();
    config.scouting.multicast.set_enabled(Some(false)).unwrap();
    config
        .listen
        .endpoints
        .set(vec!["tcp/127.0.0.1:0".parse().unwrap()])
        .unwrap();
    if namespace {
        config.insert_json5("namespace", "\"test\"").unwrap();
    }
    let server = ztimeout!(crate::open(config)).unwrap();
    let runtime = server.static_runtime().unwrap();
    let endpoint = runtime.get_locators()[0].to_string().parse().unwrap();
    let queryable = ztimeout!(server.declare_queryable("discard/query")).unwrap();
    let mut config = crate::Config::default();
    config
        .set_mode(Some(zenoh_config::WhatAmI::Client))
        .unwrap();
    config.scouting.multicast.set_enabled(Some(false)).unwrap();
    config.connect.endpoints.set(vec![endpoint]).unwrap();
    let client = ztimeout!(crate::open(config)).unwrap();
    let key = if namespace {
        "test/discard/query"
    } else {
        "discard/query"
    };
    let _replies = ztimeout!(client.get(key)).unwrap();
    let query = ztimeout!(queryable.recv_async()).unwrap();
    let router = runtime.router();
    let pending = || {
        let tables = zread!(router.tables.tables);
        let _guard = zread!(router.tables.queries_lock);
        tables
            .data
            .faces
            .values()
            .flat_map(|face| {
                face.pending_queries
                    .values()
                    .map(|(_, token)| token.clone())
            })
            .collect::<Vec<_>>()
    };
    let tokens = pending();
    assert_eq!(tokens.len(), 1);
    query.discard();
    // The dispatcher may still be returning from the delivery callback.
    ztimeout!(async {
        while !pending().is_empty() || !tokens[0].is_cancelled() {
            tokio::task::yield_now().await;
        }
    });
    ztimeout!(client.close()).unwrap();
    ztimeout!(server.close()).unwrap();
}
