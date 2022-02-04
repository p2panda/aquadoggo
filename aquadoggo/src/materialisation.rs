use p2panda_rs::document::{Document, DocumentBuilder};
use p2panda_rs::entry::EntrySigned;
use p2panda_rs::hash::Hash;
use p2panda_rs::operation::{OperationEncoded, OperationWithMeta};

use crate::db::models::{write_document, Entry as DatabaseEntry};
use crate::db::Pool;
use crate::errors::Result;

/// Materialise given document by loading all its operations from db,
/// resolving and writing the result to that schema's table
pub async fn materialise(pool: &Pool, document_id: &Hash) -> Result<Document> {
    log::info!("Materialising document {}", document_id.as_str());
    // Load operations from db
    let entries = DatabaseEntry::by_document(pool, document_id).await?;
    let operations: Vec<OperationWithMeta> = entries
        .iter()
        .filter(|database_entry| database_entry.payload_bytes.is_some())
        .map(|database_entry| {
            // Unwrap because entries in the database have already been checked to contain valid
            // operations
            let entry = EntrySigned::new(&database_entry.entry_bytes).unwrap();
            let operation =
                OperationEncoded::new(database_entry.payload_bytes.as_ref().unwrap()).unwrap();
            OperationWithMeta::new(&entry, &operation).unwrap()
        })
        .collect();

    // Resolve document
    let document = DocumentBuilder::new(operations).build()?;
    log::debug!("Materialisation yields {:?}", document.view());

    // Write document to db
    if *document.schema()
        == Hash::new("0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b")
            .unwrap()
    {
        write_document(pool, &document).await?;
    }
    Ok(document.to_owned())
}
