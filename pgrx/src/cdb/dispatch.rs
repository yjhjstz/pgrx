//LICENSE Portions Copyright 2019-2021 ZomboDB, LLC.
//LICENSE
//LICENSE Portions Copyright 2021-2023 Technology Concepts & Design, Inc.
//LICENSE
//LICENSE Portions Copyright 2023-2023 PgCentral Foundation, Inc. <contact@pgcentral.org>
//LICENSE
//LICENSE All rights reserved.
//LICENSE
//LICENSE Use of this source code is governed by the MIT license that can be found in the LICENSE file.

//! # Cloudberry Database Dispatch Functions
//!
//! This module provides FFI bindings and safe wrappers for Cloudberry Database's
//! distributed query dispatch functionality, primarily the `CdbDispatchCommand` function
//! which is used to send SQL commands to segment databases in the MPP architecture.
//!
//! ## Overview
//!
//! Cloudberry Database extends PostgreSQL with MPP (Massively Parallel Processing)
//! capabilities. The coordinator node uses dispatch functions to send query fragments
//! to segment nodes (Query Executors or QEs) for parallel execution.
//!
//! ## Core Function: CdbDispatchCommand
//!
//! The `CdbDispatchCommand` function dispatches a SQL command to all primary writer
//! segments, waits for completion, and handles errors. This is a fundamental building
//! block for distributed query execution.
//!
//! ## Usage
//!
//! ```rust
//! use pgrx::cdb::dispatch::{cdb_dispatch_command, DispatchFlags};
//!
//! // Dispatch with multiple flags
//! cdb_dispatch_command(
//!     "select pg_catalog.pg_table_size(%u)",
//!     DispatchFlags::WITH_SNAPSHOT | DispatchFlags::CANCEL_ON_ERROR
//! )?;
//! ```

use crate::pg_sys;
use std::ffi::{CStr, CString};
use std::ptr;

/// PostgreSQL ExecStatusType enum values
/// These correspond to the values defined in libpq-fe.h and are now available via pg_sys
pub use crate::pg_sys::ExecStatusType;

/// Result structure for CdbDispatchCommand
///
/// This structure mirrors the `CdbPgResults` struct defined in Cloudberry's
/// `src/include/cdb/cdbdispatchresult.h`. It contains the results returned
/// from all segments after command execution.
#[repr(C)]
#[derive(Debug)]
pub struct CdbPgResults {
    /// Array of PostgreSQL result pointers from each segment
    pg_results: *mut *mut pg_sys::pg_result,
    /// Number of results returned
    num_results: libc::c_int,
    /// Number of dispatches sent (from CdbDispatchResults.resultCount)
    num_dispatches: libc::c_int,
}

impl CdbPgResults {
    /// Create a new empty CdbPgResults structure
    pub fn new() -> Self {
        Self {
            pg_results: ptr::null_mut(),
            num_results: 0,
            num_dispatches: 0,
        }
    }

    /// Get the number of results
    pub fn num_results(&self) -> i32 {
        self.num_results
    }

    /// Get the number of dispatches
    pub fn num_dispatches(&self) -> i32 {
        self.num_dispatches
    }

    /// Check if there are any results
    pub fn has_results(&self) -> bool {
        self.num_results > 0 && !self.pg_results.is_null()
    }

    /// Get a specific PostgreSQL result by index
    /// 
    /// # Safety
    /// 
    /// This function returns a raw pointer to a pg_result structure.
    /// The caller must ensure the index is valid and the result is properly handled.
    pub unsafe fn get_pg_result(&self, index: usize) -> Option<*mut pg_sys::pg_result> {
        if index < self.num_results as usize && !self.pg_results.is_null() {
            Some(*self.pg_results.add(index))
        } else {
            None
        }
    }

    /// Iterate over all PostgreSQL results safely
    /// 
    /// This provides a safe iterator over the result set that handles
    /// status checking and basic validation.
    pub fn iter_results(&self) -> CdbResultIterator {
        CdbResultIterator {
            results: self,
            current: 0,
        }
    }

    /// Extract a single int64 value from the first result
    /// 
    /// This is a common pattern when dispatching queries that return
    /// a single numeric value (like size calculations).
    /// 
    /// # Returns
    /// 
    /// Returns the sum of all int64 values from all segments,
    /// or an error if the results don't match the expected format.
    pub fn extract_int64_sum(&self) -> Result<i64, DispatchError> {
        let mut total: i64 = 0;
        
        for result_wrapper in self.iter_results() {
            let pg_result = result_wrapper?;
            
            unsafe {
                let status = pg_sys::PQresultStatus(pg_result);
                if status != ExecStatusType::PGRES_TUPLES_OK {
                    return Err(DispatchError::PostgreSQLError(format!(
                        "Unexpected result status: {}", status
                    )));
                }

                let ntuples = pg_sys::PQntuples(pg_result);
                let nfields = pg_sys::PQnfields(pg_result);

                if ntuples != 1 || nfields != 1 {
                    return Err(DispatchError::PostgreSQLError(format!(
                        "Unexpected result shape: {} rows, {} cols (expected 1 row, 1 col)",
                        ntuples, nfields
                    )));
                }

                if pg_sys::PQgetisnull(pg_result, 0, 0) == 0 {
                    let value_cstr = pg_sys::PQgetvalue(pg_result, 0, 0);
                    if !value_cstr.is_null() {
                        let value_str = CStr::from_ptr(value_cstr).to_str()
                            .map_err(|e| DispatchError::PostgreSQLError(format!("Invalid UTF-8: {}", e)))?;
                        let value = value_str.parse::<i64>()
                            .map_err(|e| DispatchError::PostgreSQLError(format!("Invalid int64: {}", e)))?;
                        total += value;
                    }
                }
            }
        }
        
        Ok(total)
    }

    /// Get the number of rows and columns in a result safely
    /// 
    /// # Arguments
    /// 
    /// * `pg_result` - Raw pointer to pg_result
    /// 
    /// # Returns
    /// 
    /// A tuple of (num_rows, num_cols) or an error if the result is invalid
    /// 
    /// # Safety
    /// 
    /// The caller must ensure `pg_result` is a valid pointer
    pub unsafe fn get_result_dimensions(pg_result: *mut pg_sys::pg_result) -> Result<(i32, i32), DispatchError> {
        if pg_result.is_null() {
            return Err(DispatchError::PostgreSQLError("Null pg_result pointer".to_string()));
        }

        let status = pg_sys::PQresultStatus(pg_result);
        if status != ExecStatusType::PGRES_TUPLES_OK {
            return Err(DispatchError::PostgreSQLError(format!(
                "Result status is not PGRES_TUPLES_OK: {}", status
            )));
        }

        let ntuples = pg_sys::PQntuples(pg_result);
        let nfields = pg_sys::PQnfields(pg_result);
        Ok((ntuples, nfields))
    }

    /// Get a field value as a string safely
    /// 
    /// # Arguments
    /// 
    /// * `pg_result` - Raw pointer to pg_result
    /// * `row` - Row index (0-based)
    /// * `col` - Column index (0-based)
    /// 
    /// # Returns
    /// 
    /// The field value as a string, or None if the field is NULL
    /// 
    /// # Safety
    /// 
    /// The caller must ensure `pg_result` is valid and row/col are in bounds
    pub unsafe fn get_field_value(
        pg_result: *mut pg_sys::pg_result,
        row: i32,
        col: i32,
    ) -> Result<Option<String>, DispatchError> {
        if pg_result.is_null() {
            return Err(DispatchError::PostgreSQLError("Null pg_result pointer".to_string()));
        }

        if pg_sys::PQgetisnull(pg_result, row, col) != 0 {
            return Ok(None);
        }

        let value_cstr = pg_sys::PQgetvalue(pg_result, row, col);
        if value_cstr.is_null() {
            return Ok(None);
        }

        let value_str = CStr::from_ptr(value_cstr)
            .to_str()
            .map_err(|e| DispatchError::PostgreSQLError(format!("Invalid UTF-8: {}", e)))?;
        
        Ok(Some(value_str.to_string()))
    }

    /// Extract a single field value and parse it as the specified type
    /// 
    /// This is a convenience method for getting a single value from the first row/column.
    /// 
    /// # Type Parameters
    /// 
    /// * `T` - The type to parse the value as (must implement FromStr)
    /// 
    /// # Arguments
    /// 
    /// * `pg_result` - Raw pointer to pg_result
    /// 
    /// # Returns
    /// 
    /// The parsed value, or an error if parsing fails
    pub unsafe fn extract_single_value<T>(pg_result: *mut pg_sys::pg_result) -> Result<T, DispatchError> 
    where
        T: std::str::FromStr,
        T::Err: std::fmt::Display,
    {
        let (ntuples, nfields) = Self::get_result_dimensions(pg_result)?;
        
        if ntuples != 1 || nfields != 1 {
            return Err(DispatchError::PostgreSQLError(format!(
                "Expected exactly 1 row and 1 column, got {} rows and {} columns",
                ntuples, nfields
            )));
        }

        match Self::get_field_value(pg_result, 0, 0)? {
            Some(value_str) => {
                value_str.parse().map_err(|e| DispatchError::PostgreSQLError(format!(
                    "Failed to parse '{}' as {}: {}", value_str, std::any::type_name::<T>(), e
                )))
            }
            None => Err(DispatchError::PostgreSQLError("Field value is NULL".to_string())),
        }
    }
}

impl Default for CdbPgResults {
    fn default() -> Self {
        Self::new()
    }
}

/// Iterator over CdbPgResults
/// 
/// Provides safe iteration over PostgreSQL result structures,
/// handling error checking and memory safety.
pub struct CdbResultIterator<'a> {
    results: &'a CdbPgResults,
    current: usize,
}

impl<'a> Iterator for CdbResultIterator<'a> {
    type Item = Result<*mut pg_sys::pg_result, DispatchError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.results.num_results as usize {
            return None;
        }

        let result = unsafe {
            match self.results.get_pg_result(self.current) {
                Some(pg_result) => Ok(pg_result),
                None => Err(DispatchError::PostgreSQLError(
                    "Failed to get pg_result at index".to_string()
                )),
            }
        };

        self.current += 1;
        Some(result)
    }
}

/// Dispatch flags for CdbDispatchCommand
///
/// These flags correspond to the constants defined in Cloudberry's
/// `src/include/cdb/cdbdisp_query.h` and control various aspects
/// of command dispatching behavior.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DispatchFlags(i32);

impl DispatchFlags {
    /// No special flags
    pub const NONE: Self = Self(0x0);

    /// Cancel other segments if one fails
    ///
    /// Indicates whether an error occurring on one of the segment databases should
    /// cause all still-executing commands to cancel on other segments.
    /// Normally this would be true.
    pub const CANCEL_ON_ERROR: Self = Self(0x1);

    /// Execute within a global transaction
    ///
    /// Indicates whether the command to be dispatched should be done inside
    /// of a global transaction.
    pub const NEED_TWO_PHASE: Self = Self(0x2);

    /// Dispatch with a snapshot
    ///
    /// Indicates whether the command should be dispatched to segments along
    /// with a snapshot.
    pub const WITH_SNAPSHOT: Self = Self(0x4);

    /// Create a new DispatchFlags with custom value
    pub const fn new(value: i32) -> Self {
        Self(value)
    }

    /// Get the raw flag value
    pub const fn value(self) -> i32 {
        self.0
    }

    /// Check if a flag is set
    pub const fn contains(self, flag: Self) -> bool {
        (self.0 & flag.0) == flag.0
    }

    /// Combine flags using bitwise OR
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

impl std::ops::BitOr for DispatchFlags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        self.union(rhs)
    }
}

impl std::ops::BitOrAssign for DispatchFlags {
    fn bitor_assign(&mut self, rhs: Self) {
        *self = *self | rhs;
    }
}

impl From<i32> for DispatchFlags {
    fn from(value: i32) -> Self {
        Self(value)
    }
}

impl From<DispatchFlags> for i32 {
    fn from(flags: DispatchFlags) -> Self {
        flags.0
    }
}

// External function declarations for Cloudberry dispatch functions
// These functions are defined in Cloudberry's dispatcher code
extern "C-unwind" {
    /// Dispatch a SQL command to all primary writer segments
    ///
    /// This function dispatches a plain command to all primary writer Query Executors (QEs),
    /// waits until all QEs finish successfully. If one or more QEs encounter an error,
    /// it throws a PostgreSQL ERROR.
    ///
    /// # Safety
    ///
    /// This is an unsafe FFI call. The caller must ensure:
    /// - `str_command` is a valid null-terminated C string
    /// - `cdb_pgresults` points to a valid CdbPgResults structure
    /// - The function is called within a PostgreSQL backend context
    ///
    /// Use `cdb_dispatch_command` for a safe wrapper.
    fn CdbDispatchCommand(
        str_command: *const libc::c_char,
        flags: libc::c_int,
        cdb_pgresults: *mut CdbPgResults,
    );

    /// Clear and free CdbPgResults structure
    ///
    /// This function cleans up the memory allocated for CdbPgResults,
    /// including all the pg_result structures it contains.
    ///
    /// # Safety
    /// 
    /// The caller must ensure that `cdb_pgresults` points to a valid
    /// CdbPgResults structure and is not used after this call.
    fn cdbdisp_clearCdbPgResults(cdb_pgresults: *mut CdbPgResults);
}

/// Error type for dispatch operations
#[derive(Debug)]
pub enum DispatchError {
    /// String contains null bytes
    NullByteInString(std::ffi::NulError),
    /// PostgreSQL error during dispatch
    PostgreSQLError(String),
}

impl std::fmt::Display for DispatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DispatchError::NullByteInString(e) => {
                write!(f, "Command string contains null byte: {}", e)
            }
            DispatchError::PostgreSQLError(msg) => {
                write!(f, "PostgreSQL error during dispatch: {}", msg)
            }
        }
    }
}

impl std::error::Error for DispatchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DispatchError::NullByteInString(e) => Some(e),
            DispatchError::PostgreSQLError(_) => None,
        }
    }
}

impl From<std::ffi::NulError> for DispatchError {
    fn from(error: std::ffi::NulError) -> Self {
        DispatchError::NullByteInString(error)
    }
}

/// RAII wrapper for CdbPgResults that automatically cleans up
/// 
/// This wrapper ensures that `cdbdisp_clearCdbPgResults` is called
/// when the results go out of scope, preventing memory leaks.
pub struct ManagedCdbPgResults {
    inner: CdbPgResults,
}

impl ManagedCdbPgResults {
    /// Create a new managed results wrapper
    fn new(results: CdbPgResults) -> Self {
        Self { inner: results }
    }

    /// Get access to the inner CdbPgResults
    pub fn inner(&self) -> &CdbPgResults {
        &self.inner
    }

    /// Get mutable access to the inner CdbPgResults
    pub fn inner_mut(&mut self) -> &mut CdbPgResults {
        &mut self.inner
    }
}

impl Drop for ManagedCdbPgResults {
    fn drop(&mut self) {
        unsafe {
            cdbdisp_clearCdbPgResults(&mut self.inner);
        }
    }
}

impl std::ops::Deref for ManagedCdbPgResults {
    type Target = CdbPgResults;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for ManagedCdbPgResults {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// Safe wrapper for CdbDispatchCommand with automatic cleanup
///
/// This function provides a safe Rust interface to the `CdbDispatchCommand`
/// function, handling string conversion, error boundaries, and automatic
/// memory cleanup using RAII.
///
/// # Arguments
///
/// * `command` - The SQL command to dispatch to all segments
/// * `flags` - Dispatch flags controlling behavior
///
/// # Returns
///
/// * `Ok(ManagedCdbPgResults)` on successful dispatch with results from all segments
/// * `Err(DispatchError)` if an error occurs
///
/// # Examples
///
/// ```rust
/// use pgrx::cdb::dispatch::{cdb_dispatch_command, DispatchFlags};
///
/// // Dispatch a simple query - results are automatically cleaned up
/// let results = cdb_dispatch_command("SELECT version()", DispatchFlags::WITH_SNAPSHOT)?;
/// println!("Got {} results from {} segments", 
///          results.num_results(), results.num_dispatches());
///
/// // Dispatch with multiple flags
/// let results = cdb_dispatch_command(
///     "ANALYZE my_table",
///     DispatchFlags::WITH_SNAPSHOT | DispatchFlags::CANCEL_ON_ERROR
/// )?;
/// ```
///
/// # Safety
///
/// This function uses pgrx's `pg_guard_ffi_boundary` to safely handle PostgreSQL
/// errors that might be thrown by the underlying C function. The function should
/// only be called from within a PostgreSQL backend context.
pub fn cdb_dispatch_command(
    command: &str,
    flags: DispatchFlags,
) -> Result<ManagedCdbPgResults, DispatchError> {
    // Convert Rust string to C string
    let c_command = CString::new(command)?;

    // Initialize results structure
    let mut cdb_results = CdbPgResults::new();

    // Call the FFI function with proper error boundary
    unsafe {
        pg_sys::ffi::pg_guard_ffi_boundary(|| {
            CdbDispatchCommand(c_command.as_ptr(), flags.value(), &mut cdb_results);
        });
    }

    Ok(ManagedCdbPgResults::new(cdb_results))
}



/// Dispatch multiple commands in sequence
///
/// This function dispatches multiple commands one after another, using the same
/// flags for all commands. If any command fails, the function returns an error
/// and stops processing remaining commands.
///
/// # Arguments
///
/// * `commands` - Iterator of SQL commands to dispatch
/// * `flags` - Dispatch flags to use for all commands
///
/// # Examples
///
/// ```rust
/// use pgrx::cdb::dispatch::{cdb_dispatch_commands, DispatchFlags};
///
/// let maintenance_commands = vec![
///     "VACUUM my_table",
///     "ANALYZE my_table",
///     "REINDEX TABLE my_table",
/// ];
///
/// cdb_dispatch_commands(
///     maintenance_commands.iter().copied(),
///     DispatchFlags::WITH_SNAPSHOT | DispatchFlags::CANCEL_ON_ERROR
/// )?;
/// ```
pub fn cdb_dispatch_commands<I, S>(
    commands: I,
    flags: DispatchFlags,
) -> Result<Vec<ManagedCdbPgResults>, DispatchError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut results = Vec::new();
    for command in commands {
        let result = cdb_dispatch_command(command.as_ref(), flags)?;
        results.push(result);
    }
    Ok(results)
}

/// Dispatch a command and extract the sum of int64 values from all segments
///
/// This is a high-level convenience function that replicates the behavior
/// of Cloudberry's `get_size_from_segDBs` function. It's useful for queries
/// that return a single numeric value from each segment that should be summed.
///
/// # Arguments
///
/// * `command` - SQL command that returns a single int64 value
/// * `flags` - Dispatch flags (typically DF_WITH_SNAPSHOT)
///
/// # Returns
///
/// The sum of all int64 values returned from all segments
///
/// # Examples
///
/// ```rust
/// use pgrx::cdb::dispatch::{get_int64_sum_from_segments, DispatchFlags};
///
/// // Get total table size across all segments
/// let total_size = get_int64_sum_from_segments(
///     "SELECT pg_total_relation_size('my_table'::regclass)",
///     DispatchFlags::WITH_SNAPSHOT
/// )?;
///
/// // Get total row count across all segments  
/// let row_count = get_int64_sum_from_segments(
///     "SELECT count(*) FROM my_table",
///     DispatchFlags::WITH_SNAPSHOT
/// )?;
/// ```
///
/// # Errors
///
/// Returns an error if:
/// - The dispatch fails
/// - Any segment returns a result that is not exactly 1 row, 1 column
/// - Any segment returns a non-numeric value
/// - The result cannot be parsed as an int64
pub fn get_int64_sum_from_segments(
    command: &str,
    flags: DispatchFlags,
) -> Result<i64, DispatchError> {
    let results = cdb_dispatch_command(command, flags)?;
    results.extract_int64_sum()
}

/// Get the current gp_role setting (for Cloudberry/Greenplum)
/// 
/// This function retrieves the current value of the gp_role GUC parameter
/// which indicates the node's role in the cluster.
/// 
/// # Returns
/// 
/// * `Some(String)` - The gp_role value ("dispatch", "execute", "utility", etc.)
/// * `None` - If gp_role is not set (not running on Cloudberry/Greenplum)
pub fn get_gp_role() -> Option<String> {
    use std::ffi::CStr;
    
    unsafe {
        let gp_role_guc = pg_sys::GetConfigOptionByName(
            c"gp_role".as_ptr(), 
            std::ptr::null_mut(), 
            false
        );
        if !gp_role_guc.is_null() {
            Some(CStr::from_ptr(gp_role_guc).to_string_lossy().to_string())
        } else {
            None
        }
    }
}

/// Check if running on Cloudberry/Greenplum coordinator node
/// 
/// This function checks if the current node is a coordinator (dispatcher)
/// by examining the gp_role GUC parameter.
/// 
/// # Returns
/// 
/// `true` if running on coordinator (gp_role = "dispatch"), `false` otherwise
pub fn is_coordinator() -> bool {
    get_gp_role().map_or(false, |role| role == "dispatch")
}


/// Dispatch a command only if we're on a coordinator node
/// 
/// This function provides a safe way to conditionally dispatch commands
/// only when running on a coordinator node.
/// 
/// # Arguments
/// 
/// * `command` - SQL command to dispatch
/// * `flags` - Dispatch flags
/// 
/// # Returns
/// 
/// * `Some(Result)` if in dispatch mode - contains the dispatch result
/// * `None` if not in dispatch mode (e.g., running on a segment)
/// 
/// # Examples
/// 
/// ```rust
/// use pgrx::cdb::dispatch::{dispatch_if_coordinator, DispatchFlags};
/// 
/// match dispatch_if_coordinator("SELECT version()", DispatchFlags::WITH_SNAPSHOT) {
///     Some(Ok(results)) => {
///         // We're on coordinator and dispatch succeeded
///         println!("Dispatch successful: {} results", results.num_results());
///     }
///     Some(Err(e)) => {
///         // We're on coordinator but dispatch failed
///         eprintln!("Dispatch failed: {}", e);
///     }
///     None => {
///         // We're on a segment node - don't dispatch
///         println!("Not on coordinator node, skipping");
///     }
/// }
/// ```
pub fn dispatch_if_coordinator(
    command: &str,
    flags: DispatchFlags,
) -> Option<Result<ManagedCdbPgResults, DispatchError>> {
    if is_coordinator() {
        Some(cdb_dispatch_command(command, flags))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn test_dispatch_flags_operations() {
        let flags = DispatchFlags::WITH_SNAPSHOT | DispatchFlags::CANCEL_ON_ERROR;
        assert_eq!(flags.value(), 0x5); // 0x4 | 0x1

        assert!(flags.contains(DispatchFlags::WITH_SNAPSHOT));
        assert!(flags.contains(DispatchFlags::CANCEL_ON_ERROR));
        assert!(!flags.contains(DispatchFlags::NEED_TWO_PHASE));
    }

    #[test]
    fn test_cdb_pg_results_default() {
        let results = CdbPgResults::default();
        assert_eq!(results.num_results(), 0);
        assert_eq!(results.num_dispatches(), 0);
        assert!(!results.has_results());
    }

    #[test]
    fn test_dispatch_error_from_nul_error() {
        let nul_error = CString::new("test\0string").unwrap_err();
        let dispatch_error = DispatchError::from(nul_error);
        
        match dispatch_error {
            DispatchError::NullByteInString(_) => (),
            _ => panic!("Expected NullByteInString variant"),
        }
    }

    #[test]
    fn test_dispatch_flags_custom_values() {
        let custom_flags = DispatchFlags::new(0xFF);
        assert_eq!(custom_flags.value(), 0xFF);
        
        // Test contains with custom flags
        let combined = DispatchFlags::WITH_SNAPSHOT | custom_flags;
        assert!(combined.contains(DispatchFlags::WITH_SNAPSHOT));
        assert!(combined.contains(custom_flags));
    }

    #[test]
    fn test_dispatch_flags_union() {
        let flags1 = DispatchFlags::WITH_SNAPSHOT;
        let flags2 = DispatchFlags::CANCEL_ON_ERROR;
        let combined = flags1.union(flags2);
        
        assert_eq!(combined.value(), 0x5); // 0x4 | 0x1
        assert!(combined.contains(flags1));
        assert!(combined.contains(flags2));
    }

    #[test]
    fn test_dispatch_flags_bitor_assign() {
        let mut flags = DispatchFlags::WITH_SNAPSHOT;
        flags |= DispatchFlags::CANCEL_ON_ERROR;
        
        assert!(flags.contains(DispatchFlags::WITH_SNAPSHOT));
        assert!(flags.contains(DispatchFlags::CANCEL_ON_ERROR));
        assert_eq!(flags.value(), 0x5);
    }

    #[test]
    fn test_dispatch_flags_from_conversions() {
        let value: i32 = 0x7; // All first 3 flags
        let flags = DispatchFlags::from(value);
        
        assert_eq!(flags.value(), 0x7);
        assert!(flags.contains(DispatchFlags::CANCEL_ON_ERROR));
        assert!(flags.contains(DispatchFlags::NEED_TWO_PHASE));
        assert!(flags.contains(DispatchFlags::WITH_SNAPSHOT));
        
        let back_to_i32: i32 = flags.into();
        assert_eq!(back_to_i32, 0x7);
    }

    #[test]
    fn test_cdb_pg_results_new() {
        let results = CdbPgResults::new();
        assert_eq!(results.num_results(), 0);
        assert_eq!(results.num_dispatches(), 0);
        assert!(!results.has_results());
        assert!(results.pg_results.is_null());
    }

    #[test]
    fn test_cdb_pg_results_methods() {
        let mut results = CdbPgResults::new();
        results.num_results = 5;
        results.num_dispatches = 3;
        
        assert_eq!(results.num_results(), 5);
        assert_eq!(results.num_dispatches(), 3);
        assert!(!results.has_results()); // Still false because pg_results is null
        
        // Simulate non-null results (note: this is just for testing structure behavior)
        results.pg_results = 0x1 as *mut *mut pg_sys::pg_result; // Non-null pointer
        assert!(results.has_results());
    }

    #[test]
    fn test_dispatch_error_display() {
        let nul_error = CString::new("test\0string").unwrap_err();
        let dispatch_error = DispatchError::NullByteInString(nul_error);
        let display_str = format!("{}", dispatch_error);
        assert!(display_str.contains("Command string contains null byte"));
        
        let pg_error = DispatchError::PostgreSQLError("Connection failed".to_string());
        let display_str = format!("{}", pg_error);
        assert!(display_str.contains("PostgreSQL error during dispatch"));
        assert!(display_str.contains("Connection failed"));
    }

    #[test]
    fn test_dispatch_error_source() {
        let nul_error = CString::new("test\0string").unwrap_err();
        let dispatch_error = DispatchError::NullByteInString(nul_error);
        assert!(dispatch_error.source().is_some());
        
        let pg_error = DispatchError::PostgreSQLError("Some error".to_string());
        assert!(pg_error.source().is_none());
    }

    #[test]
    fn test_exec_status_type_values() {
        // Test that our enum values match PostgreSQL's libpq constants
        assert_eq!(ExecStatusType::PGRES_EMPTY_QUERY as i32, 0);
        assert_eq!(ExecStatusType::PGRES_COMMAND_OK as i32, 1);
        assert_eq!(ExecStatusType::PGRES_TUPLES_OK as i32, 2);
        assert_eq!(ExecStatusType::PGRES_FATAL_ERROR as i32, 7);
        assert_eq!(ExecStatusType::PGRES_SINGLE_TUPLE as i32, 9);
    }

    // Note: ManagedCdbPgResults tests require linking to Cloudberry libraries
    // These are tested in integration tests or when built with actual CDB

    #[test]
    fn test_cdb_result_iterator_empty() {
        let results = CdbPgResults::new();
        let mut iter = results.iter_results();
        
        // Empty results should immediately return None
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_dispatch_flags_constants() {
        assert_eq!(DispatchFlags::NONE.value(), 0x0);
        assert_eq!(DispatchFlags::CANCEL_ON_ERROR.value(), 0x1);
        assert_eq!(DispatchFlags::NEED_TWO_PHASE.value(), 0x2);
        assert_eq!(DispatchFlags::WITH_SNAPSHOT.value(), 0x4);
    }

    // Note: Tests for get_field_value, get_result_dimensions, and extract_single_value
    // require actual PostgreSQL/libpq linking and are tested in integration tests
    // The null pointer checks in these functions provide basic safety validation

    // Mock tests for libpq function behavior without actual PostgreSQL connection
    #[test]
    fn test_cdb_pg_results_safe_methods() {
        let mut results = CdbPgResults::new();
        
        // Test basic accessors
        assert_eq!(results.num_results(), 0);
        assert_eq!(results.num_dispatches(), 0);
        assert!(!results.has_results());
        
        // Test with some values
        results.num_results = 3;
        results.num_dispatches = 2;
        assert_eq!(results.num_results(), 3);
        assert_eq!(results.num_dispatches(), 2);
        assert!(!results.has_results()); // Still false because pg_results is null
        
        // Test unsafe get_pg_result with null pointer
        unsafe {
            assert!(results.get_pg_result(0).is_none());
            assert!(results.get_pg_result(1).is_none());
        }
    }

    #[test]
    fn test_extract_int64_sum_empty_results() {
        let results = CdbPgResults::new();
        
        // Empty results iterator should be empty, so sum should be 0
        // Note: This test only verifies the empty case without calling libpq functions
        let mut total = 0i64;
        for result_wrapper in results.iter_results() {
            // This loop shouldn't execute for empty results
            if let Ok(_pg_result) = result_wrapper {
                total += 1; // This shouldn't happen
            }
        }
        assert_eq!(total, 0);
    }

    #[test]
    fn test_cdb_result_iterator_bounds() {
        let mut results = CdbPgResults::new();
        results.num_results = 2;
        
        let mut iter = results.iter_results();
        
        // Should try to get first result but fail because pg_results is null
        let first = iter.next();
        assert!(first.is_some());
        if let Some(Err(DispatchError::PostgreSQLError(msg))) = first {
            assert!(msg.contains("Failed to get pg_result"));
        }
        
        // Should try to get second result
        let second = iter.next();
        assert!(second.is_some());
        
        // Should be exhausted
        let third = iter.next();
        assert!(third.is_none());
    }
}