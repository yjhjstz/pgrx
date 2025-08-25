//LICENSE Portions Copyright 2019-2021 ZomboDB, LLC.
//LICENSE
//LICENSE Portions Copyright 2021-2023 Technology Concepts & Design, Inc.
//LICENSE
//LICENSE Portions Copyright 2023-2023 PgCentral Foundation, Inc. <contact@pgcentral.org>
//LICENSE
//LICENSE All rights reserved.
//LICENSE
//LICENSE Use of this source code is governed by the MIT license that can be found in the LICENSE file.

//! # Cloudberry Database (CDB) Support Module
//!
//! This module provides support for Cloudberry Database specific functionality,
//! including distributed query execution and MPP (Massively Parallel Processing) features.
//!
//! Cloudberry Database extends PostgreSQL with distributed computing capabilities,
//! and this module provides safe Rust bindings for interacting with those features.

pub mod dispatch;

pub use dispatch::*;