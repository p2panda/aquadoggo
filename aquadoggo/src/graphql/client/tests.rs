// SPDX-License-Identifier: AGPL-3.0-or-later

//! Integration tests for dynamic graphql api

use futures::Future;
use log::debug;
use p2panda_rs::document::Document;
use p2panda_rs::operation::{Operation, OperationValue};
use p2panda_rs::schema::FieldType;
use p2panda_rs::test_utils::fixtures::create_operation;
use rstest::{fixture, rstest};

use crate::db::provider::SqlStorage;
use crate::db::stores::test_utils::{
    with_db_manager_teardown, AsyncTestFn, TestDatabase, TestDatabaseManager, TestDatabaseRunner,
};

struct SchemaSpec<'a>(&'a str, Vec<(&'a str, FieldType)>);
struct DocumentSpec<'a>(&'a str, Vec<Operation>);

#[fixture]
fn test_db_with_documents<'a>(
    #[default(vec![])] schemas_spec: Vec<SchemaSpec<'a>>,
    #[default(vec![])] documents_spec: Vec<DocumentSpec<'a>>,
) -> TestDocumentDatabaseRunner<'a> {
    TestDocumentDatabaseRunner {
        schemas_spec,
        documents_spec,
    }
}

struct TestDocumentDatabaseRunner<'a> {
    schemas_spec: Vec<SchemaSpec<'a>>,
    documents_spec: Vec<DocumentSpec<'a>>,
}

impl<'a> TestDocumentDatabaseRunner<'a> {
    pub fn run<F: AsyncTestFn + Send + Sync + 'static>(&self, test: F) {
        with_db_manager_teardown(move |manager: TestDatabaseManager| async move {
            let mut db = manager.create("sqlite::memory:").await;

            Self::create_schemas(db.store.clone());
            Self::create_documents(db.store.clone());

            test.call(db).await;
            debug!("All done")
        });
    }

    fn create_schemas(store: SqlStorage) {
        debug!("Setting up schemas")
    }

    fn create_documents(store: SqlStorage) {
        debug!("Setting up documents")
    }
}

#[rstest]
fn new_test(
    #[from(test_db_with_documents)]
    #[with(
        vec![
            SchemaSpec("schema_name", vec![("field_name", FieldType::Bool)]),
        ],
        vec![
            DocumentSpec("schema_name", vec![
                create_operation(&vec![("field_name", OperationValue::Boolean(true))])
            ])
        ]
    )]
    runner: TestDocumentDatabaseRunner,
) {
    runner.run(&|db: TestDatabase| async move {
        debug!("In test");
    });
}
