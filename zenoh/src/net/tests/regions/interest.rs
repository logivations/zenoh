//
// Copyright (c) 2026 ZettaScale Technology
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

//! Tests involving [`zenoh_protocol::network::interest`].

use zenoh_protocol::{
    core::{Bound, Region, WhatAmI},
    network::{
        declare::{queryable::ext::QueryableInfoType, DeclareToken, TokenId},
        interest::{self, InterestMode, InterestOptions},
        Interest,
    },
};

use super::{try_init_tracing_subscriber, Connection, FaceDef, Harness, HarnessBuilder};
#[cfg(debug_assertions)]
use crate::net::routing::dispatcher::interests::REMOTE_INTEREST_SCAN_COUNT;
use crate::net::{primitives::Primitives, routing::hat::peer::INITIAL_INTEREST_ID};

/// Test that current tokens are re-propagated even if they've already been propagated in future
/// mode.
///
/// ```d2
/// shape: sequence_diagram
///
/// C1 -> R: Interest id=1 mode=F
///
/// C2 -> R.1: DeclareToken iid=None
/// R.1 -> C1: DeclareToken iid=None
///
/// C1 -> R.2: Interest id=2 mode=C
/// R.2 -> C1: DeclareToken iid=2
/// R.2 -> C1: DeclareFinal iid=2
/// ```
#[test]
fn test_current_token_repropagation() {
    try_init_tracing_subscriber();

    let r = Harness::new_router();
    let c1 = Harness::new_client();
    let c2 = Harness::new_client();

    let s1 = c1.new_session();
    let s2 = c1.new_session();

    let r_face = FaceDef::default()
        .region(Region::North)
        .remote_bound(Bound::South)
        .mode(WhatAmI::Router);

    let c_face = FaceDef::default()
        .mode(WhatAmI::Client)
        .region(Region::default_south(WhatAmI::Client));

    let mut c1_r = Connection {
        a: &c1,
        a2b: r_face,
        b: &r,
        b2a: c_face,
    }
    .establish();
    c1_r.bi_fwd();

    let mut c2_r = Connection {
        a: &c2,
        a2b: r_face,
        b: &r,
        b2a: c_face,
    }
    .establish();
    c2_r.bi_fwd();

    s1.interest(1, InterestMode::Future, InterestOptions::TOKENS, "test");
    c1_r.bi_fwd();

    s2.declare_token(None, 1, "test");
    c2_r.bi_fwd();
    c1_r.bi_fwd();

    assert_eq!(
        s1.recorder().tokens().as_slice(),
        &[DeclareToken {
            id: 1,
            wire_expr: "test".into(),
        }]
    );

    s1.interest(1, InterestMode::Current, InterestOptions::TOKENS, "test");
    c1_r.bi_fwd();

    assert_eq!(
        s1.recorder().tokens().as_slice(),
        &[
            DeclareToken {
                id: 1,
                wire_expr: "test".into(),
            },
            DeclareToken {
                id: TokenId::default(),
                wire_expr: "test".into(),
            }
        ]
    );
}

/// Test peer-to-peer interest routing in the presence of unfinalized initial interests.
///
/// This checks for a regression discovered in RMW Zenoh which uses peer mode and sends a
/// [liveliness GET] right after opening a session.
///
/// This issue occurred because we did not check that the source of a current token's interest is
/// south-bound before propagating it to peers with unfinalized initial interests.
///
/// [liveliness GET]:
///     https://github.com/ros2/rmw_zenoh/blob/944a8715f5af6f58e74e318d31510409f69a5e6e/rmw_zenoh_cpp/src/detail/rmw_context_impl_s.cpp#L250-L254
#[test]
fn test_p2p_interest_routing_with_unfinalized_initial_interests() {
    try_init_tracing_subscriber();

    let p = HarnessBuilder::new()
        .mode(WhatAmI::Peer)
        .subregions([])
        .build();

    let p0 = p.new_face(FaceDef::default().mode(WhatAmI::Peer));
    let p1 = p.new_face(FaceDef::default().mode(WhatAmI::Peer));

    assert_eq!(p0.recorder().declare_finals().len(), 1);
    assert_eq!(p1.recorder().declare_finals().len(), 1);

    p0.interest_wildcard(42, InterestMode::Current, InterestOptions::TOKENS);

    assert_eq!(p0.recorder().tokens().len(), 0);

    assert_eq!(p0.recorder().declare_finals().len(), 2);
    assert_eq!(p1.recorder().declare_finals().len(), 1);

    assert!(p0.recorder().interests().is_empty());
    assert!(p1.recorder().interests().is_empty());
}

/// Same as [`test_p2p_interest_routing_with_unfinalized_initial_interests`] but finalizes initial
/// interests before sending the current tokens interest.
#[test]
fn test_p2p_interest_routing_with_finalized_initial_interests() {
    let p = HarnessBuilder::new()
        .mode(WhatAmI::Peer)
        .subregions([])
        .build();

    let p0 = p.new_face(FaceDef::default().mode(WhatAmI::Peer));
    let p1 = p.new_face(FaceDef::default().mode(WhatAmI::Peer));

    p0.declare_final(0);
    p1.declare_final(0);

    assert_eq!(p0.recorder().declare_finals().len(), 1);
    assert_eq!(p1.recorder().declare_finals().len(), 1);

    p0.interest_wildcard(42, InterestMode::Current, InterestOptions::TOKENS);

    assert_eq!(p0.recorder().tokens().len(), 0);

    assert_eq!(p0.recorder().declare_finals().len(), 2);
    assert_eq!(p1.recorder().declare_finals().len(), 1);

    assert!(p0.recorder().interests().is_empty());
    assert!(p1.recorder().interests().is_empty());
}

/// Concurrent current-future interest in a two-region hierarchy.
#[test_case::test_matrix(
    [WhatAmI::Client, WhatAmI::Peer],
    [WhatAmI::Client, WhatAmI::Peer]
)]
fn test_concurrent_current_future_interests(north: WhatAmI, south: WhatAmI) {
    try_init_tracing_subscriber();

    let r = Region::default_south(south);
    let g = HarnessBuilder::new().mode(north).subregions([r]).build();
    let n = g.new_face(FaceDef::default().remote_bound(Bound::South));
    let s0 = g.new_face(FaceDef::default().mode(south).region(r));
    let s1 = g.new_face(FaceDef::default().mode(south).region(r));

    assert_eq!(n.recorder().interests().len(), 0);

    s0.interest_wildcard(
        42,
        InterestMode::CurrentFuture,
        InterestOptions::KEYEXPRS + InterestOptions::SUBSCRIBERS,
    );

    s1.interest_wildcard(
        42,
        InterestMode::CurrentFuture,
        InterestOptions::KEYEXPRS + InterestOptions::SUBSCRIBERS,
    );

    assert_eq!(n.recorder().interests().len(), 2);

    n.declare_subscriber(Some(42), 1999, "k");

    assert_eq!(s0.recorder().subscribers().len(), 1);
    assert_eq!(s1.recorder().subscribers().len(), 1);

    let shared = n.recorder().interests();
    for interest in &shared {
        n.declare_final(interest.id);
    }
    // Options and resource are both part of incoming and outgoing identity.
    s0.interest_wildcard(43, InterestMode::Future, InterestOptions::QUERYABLES);
    s0.interest(
        44,
        InterestMode::Future,
        InterestOptions::KEYEXPRS + InterestOptions::SUBSCRIBERS,
        "other/@verbatim",
    );
    n.recorder().clear();

    s0.interest_wildcard(42, InterestMode::Final, InterestOptions::empty());
    assert!(n.recorder().interests().is_empty());
    s1.interest_wildcard(42, InterestMode::Final, InterestOptions::ALL);

    let mut expected: Vec<_> = shared
        .iter()
        .map(|original| Interest {
            id: original.id,
            mode: InterestMode::Final,
            options: original.options,
            wire_expr: None,
            ext_qos: interest::ext::QoSType::INTEREST,
            ext_tstamp: None,
            ext_nodeid: interest::ext::NodeIdType::DEFAULT,
        })
        .collect();
    let mut finals = n.recorder().interests();
    expected.sort_by_key(|interest| interest.id);
    finals.sort_by_key(|interest| interest.id);
    assert_eq!(finals, expected);
    {
        let tables = g.gateway.tables.tables.read().unwrap();
        assert_eq!(tables.hats[r].remote_interests(&tables.data).len(), 2);
        let outgoing = &tables.data.faces[&n.face.state.id].local_interests;
        assert_eq!(outgoing.len(), 2);
        assert!(shared
            .iter()
            .all(|interest| !outgoing.contains_key(&interest.id)));
        assert!(!s0.face.state.remote_key_interests.contains_key(&42));
        assert!(!s1.face.state.remote_key_interests.contains_key(&42));
    }

    // An unknown/duplicate Final still has no effects and performs no ownership scan.
    #[cfg(debug_assertions)]
    REMOTE_INTEREST_SCAN_COUNT.with(|count| count.set(0));
    s0.interest_wildcard(42, InterestMode::Final, InterestOptions::ALL);
    assert_eq!(n.recorder().interests().len(), 2);
    #[cfg(debug_assertions)]
    REMOTE_INTEREST_SCAN_COUNT.with(|count| assert_eq!(count.get(), 0));
}

/// Re-propagated current-future interest in a two-region hierarchy.
#[test_case::test_matrix(
    [WhatAmI::Client, WhatAmI::Peer],
    [WhatAmI::Client, WhatAmI::Peer]
)]
/// Test that current-future interests are propagated upstresam on open and that downstream
/// declarations with interest id are accepted by the middle gateway even though there is no
/// breadcrumb.
fn test_current_future_interest_propagation_on_open(north: WhatAmI, south: WhatAmI) {
    try_init_tracing_subscriber();

    let r = Region::default_south(south);
    let g = HarnessBuilder::new().mode(north).subregions([r]).build();
    let s = g.new_face(FaceDef::default().mode(south).region(r));

    s.interest_wildcard(42, InterestMode::CurrentFuture, InterestOptions::QUERYABLES);
    s.interest(
        43,
        InterestMode::Future,
        InterestOptions::QUERYABLES,
        "retired/**",
    );
    s.interest_wildcard(43, InterestMode::Final, InterestOptions::empty());

    let n = g.new_face(FaceDef::default().remote_bound(Bound::South));

    assert_eq!(n.recorder().interests().len(), 1);
    assert_eq!(s.recorder().queryables().len(), 0);

    n.declare_queryable(Some(42), 1999, "k", QueryableInfoType::DEFAULT);

    assert_eq!(s.recorder().queryables().len(), 1);

    // Closing the gateway preserves downstream interests for replay. Retire one while
    // disconnected, then reconnect: only the surviving exact expression is replayed.
    n.face.send_close();
    s.interest(
        44,
        InterestMode::Future,
        InterestOptions::QUERYABLES,
        "kept/@verbatim",
    );
    s.interest_wildcard(42, InterestMode::Final, InterestOptions::empty());
    let reconnected = g.new_face(FaceDef::default().remote_bound(Bound::South));
    let replayed = reconnected.recorder().interests();
    assert_eq!(replayed.len(), 1);
    assert_eq!(replayed[0].mode, InterestMode::CurrentFuture);
    assert_eq!(replayed[0].options, InterestOptions::QUERYABLES);
    let tables = g.gateway.tables.tables.read().unwrap();
    let outgoing = &tables.data.faces[&reconnected.face.state.id].local_interests;
    assert_eq!(outgoing.len(), 1);
    assert_eq!(
        outgoing[&replayed[0].id].res.as_ref().unwrap().expr(),
        "kept/@verbatim"
    );
    assert_eq!(tables.hats[r].remote_interests(&tables.data).len(), 1);
}

/// Test that gateways send back declare final if there is no upstream.
#[test_case::test_matrix([WhatAmI::Client, WhatAmI::Peer, WhatAmI::Router])]
fn test_current_interest_finalization(mode: WhatAmI) {
    let g = HarnessBuilder::new()
        .mode(mode)
        .subregions([Region::Local])
        .build();

    let s = g.new_session();

    s.interest_wildcard(2, InterestMode::CurrentFuture, InterestOptions::ALL);

    assert_eq!(s.recorder().declare_finals().len(), 1);
    #[cfg(debug_assertions)]
    REMOTE_INTEREST_SCAN_COUNT.with(|count| count.set(0));
    s.interest_wildcard(2, InterestMode::Final, InterestOptions::empty());
    #[cfg(debug_assertions)]
    REMOTE_INTEREST_SCAN_COUNT.with(|count| assert_eq!(count.get(), 0));
    let tables = g.gateway.tables.tables.read().unwrap();
    assert!(tables.hats[Region::Local]
        .remote_interests(&tables.data)
        .is_empty());
    assert!(!s.face.state.remote_key_interests.contains_key(&2));
}

/// Removal without an upstream destination still clears owner and peer entity state.
#[test_case::test_matrix(
    [WhatAmI::Client, WhatAmI::Peer, WhatAmI::Router],
    [WhatAmI::Client, WhatAmI::Peer]
)]
fn test_interest_final_without_gateway(north: WhatAmI, south: WhatAmI) {
    let r = Region::default_south(south);
    let g = HarnessBuilder::new()
        .mode(north)
        .subregions([r, Region::Local])
        .build();
    let other_peer = (north == WhatAmI::Peer).then(|| {
        g.new_face(
            FaceDef::default()
                .mode(WhatAmI::Peer)
                .remote_bound(Bound::North),
        )
    });
    let s0 = g.new_face(FaceDef::default().mode(south).region(r));
    let s1 = g.new_face(FaceDef::default().mode(south).region(r));
    let entities = g.new_session();
    entities.declare_subscriber(None, 10, "cleanup/item");
    entities.declare_queryable(None, 11, "cleanup/item", QueryableInfoType::DEFAULT);
    let options =
        InterestOptions::KEYEXPRS + InterestOptions::SUBSCRIBERS + InterestOptions::QUERYABLES;
    for source in [&s0, &s1] {
        source.interest(42, InterestMode::CurrentFuture, options, "cleanup/**");
        assert_eq!(source.recorder().subscribers().len(), 1);
        assert_eq!(source.recorder().queryables().len(), 1);
    }
    let old_subscriber = s0.recorder().subscribers()[0].id;
    let old_queryable = s0.recorder().queryables()[0].id;
    s0.recorder().clear();

    #[cfg(debug_assertions)]
    REMOTE_INTEREST_SCAN_COUNT.with(|count| count.set(0));
    s0.interest_wildcard(42, InterestMode::Final, InterestOptions::empty());
    // Reusing a removed ID must create fresh peer subscriber/queryable registrations.
    s0.interest(42, InterestMode::CurrentFuture, options, "cleanup/**");
    assert_eq!(s0.recorder().subscribers().len(), 1);
    assert_eq!(s0.recorder().queryables().len(), 1);
    if south == WhatAmI::Peer {
        assert_ne!(s0.recorder().subscribers()[0].id, old_subscriber);
        assert_ne!(s0.recorder().queryables()[0].id, old_queryable);
    }
    s0.interest_wildcard(42, InterestMode::Final, InterestOptions::empty());
    s1.interest_wildcard(42, InterestMode::Final, InterestOptions::empty());
    #[cfg(debug_assertions)]
    REMOTE_INTEREST_SCAN_COUNT.with(|count| assert_eq!(count.get(), 0));

    let tables = g.gateway.tables.tables.read().unwrap();
    assert!(tables.hats[r].remote_interests(&tables.data).is_empty());
    assert!(!s0.face.state.remote_key_interests.contains_key(&42));
    assert!(!s1.face.state.remote_key_interests.contains_key(&42));
    if let Some(peer) = &other_peer {
        assert!(peer.recorder().interests().is_empty());
        let initial = &tables.data.faces[&peer.face.state.id].local_interests;
        assert_eq!(initial.len(), 1);
        assert!(initial.contains_key(&INITIAL_INTEREST_ID));
    }
}

/// Both future modes retain the outgoing group until the last future owner leaves.
#[test_case::test_matrix(
    [WhatAmI::Client, WhatAmI::Peer],
    [WhatAmI::Client, WhatAmI::Peer],
    [false, true]
)]
fn test_interest_final_mixed_modes(north: WhatAmI, south: WhatAmI, remove_history: bool) {
    let r = Region::default_south(south);
    let g = HarnessBuilder::new().mode(north).subregions([r]).build();
    let n = g.new_face(FaceDef::default().remote_bound(Bound::South));
    let s = g.new_face(FaceDef::default().mode(south).region(r));
    for (id, mode) in [
        (42, InterestMode::Future),
        (43, InterestMode::CurrentFuture),
    ] {
        s.interest(id, mode, InterestOptions::QUERYABLES, "mode/**");
    }
    let outgoing = n.recorder().interests();
    assert_eq!(outgoing.len(), 2);
    let current_id = outgoing
        .iter()
        .find(|interest| interest.mode == InterestMode::CurrentFuture)
        .unwrap()
        .id;
    {
        let tables = g.gateway.tables.tables.read().unwrap();
        // Preserve distinct stored modes and replay equality, not just one merged record.
        assert_eq!(tables.hats[r].remote_interests(&tables.data).len(), 2);
        assert!(tables.data.faces[&n.face.state.id]
            .pending_current_interests
            .contains_key(&current_id));
    }
    let (removed, last, survivor_mode) = if remove_history {
        (43, 42, InterestMode::Future)
    } else {
        (42, 43, InterestMode::CurrentFuture)
    };
    n.recorder().clear();
    s.interest_wildcard(removed, InterestMode::Final, InterestOptions::empty());
    assert!(n.recorder().interests().is_empty());
    {
        let tables = g.gateway.tables.tables.read().unwrap();
        let remaining = tables.hats[r].remote_interests(&tables.data);
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining.iter().next().unwrap().mode, survivor_mode);
        let face = &tables.data.faces[&n.face.state.id];
        assert_eq!(face.local_interests.len(), 2);
        assert!(face.pending_current_interests.contains_key(&current_id));
    }

    // Finishing the current snapshot is independent of remaining future ownership.
    n.declare_final(current_id);
    assert_eq!(s.recorder().declare_finals().len(), 1);
    {
        let tables = g.gateway.tables.tables.read().unwrap();
        let face = &tables.data.faces[&n.face.state.id];
        assert!(!face.pending_current_interests.contains_key(&current_id));
        assert_eq!(face.local_interests.len(), 2);
        assert!(face.local_interests[&current_id].finalized);
    }
    s.interest_wildcard(last, InterestMode::Final, InterestOptions::empty());
    let mut finals = n.recorder().interests();
    let mut expected: Vec<_> = outgoing
        .iter()
        .map(|original| Interest {
            id: original.id,
            mode: InterestMode::Final,
            options: original.options,
            wire_expr: None,
            ext_qos: interest::ext::QoSType::INTEREST,
            ext_tstamp: None,
            ext_nodeid: interest::ext::NodeIdType::DEFAULT,
        })
        .collect();
    finals.sort_by_key(|interest| interest.id);
    expected.sort_by_key(|interest| interest.id);
    assert_eq!(finals, expected);
    let tables = g.gateway.tables.tables.read().unwrap();
    assert!(tables.hats[r].remote_interests(&tables.data).is_empty());
    assert!(tables.data.faces[&n.face.state.id]
        .local_interests
        .is_empty());
}
