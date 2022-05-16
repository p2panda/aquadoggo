mod document;
mod operation;

pub use document::{AsStorageDocument, AsStorageDocumentView, DocumentStore};
pub use operation::{AsStorageOperation, OperationStore, PreviousOperations};
