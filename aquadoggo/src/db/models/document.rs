use p2panda_rs::document::{Document, DocumentView};
use p2panda_rs::operation::OperationValue;
use sqlx::{query, query_as, FromRow};

use crate::db::Pool;
use crate::errors::Result;

#[derive(FromRow)]
pub struct Bookmark {
    pub document: String,
    pub previous_operations: String,
    pub created: String,
    pub url: String,
    pub title: String,
}

fn build_insert_query(document: &Document) -> String {
    // The number of columns in the document table that don't represent fields of the document but
    // metadata such as the document's id and previous operations.
    const NUM_FIXED_ARGS: usize = 2;

    // The field spec enumerates all schema-specific vield names
    let field_spec = document
        .view()
        .keys()
        .into_iter()
        .reduce(|acc, val| format!("{}, {}", acc, val))
        .unwrap();

    // The parameter spec is a list of parameter placeholders with as many elements as there are
    // columns to insert
    let parameter_spec = (0..document.view().len())
        // Add 2 because paramas are 1-indexed and "document" is
        // a static parameter inserted before these dynamic ones
        .map(|i| format!("${}", (i + NUM_FIXED_ARGS + 1)))
        .reduce(|acc, elem| format!("{}, {}", acc, elem))
        .unwrap();

    let update_spec = match document.view() {
        DocumentView::Available(fields) => {
            fields
            .iter()
            .enumerate()
            // Make key-binding pairs like "key=$2"
            .map(|(i, (key, _))| format!("{}=${}", key, (i + NUM_FIXED_ARGS + 1)))
            // Concatenate
            .reduce(|acc, val| format!("{},\n{}", acc, val))
            .unwrap()
        },
        DocumentView::Deleted(fields) => {
            fields
            .iter()
            .enumerate()
            // Make key-binding pairs like "key=$2"
            .map(|(i, key)| format!("{}=${}", key, (i + NUM_FIXED_ARGS + 1)))
            // Concatenate
            .reduce(|acc, val| format!("{},\n{}", acc, val))
            .unwrap()
        }
    };


    format!(
        "
        INSERT INTO
            `bookmarks-0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b`
            (document, previous_operations, {})
        VALUES
            ($1, $2, {})
        ON CONFLICT (`document`) DO UPDATE SET
            previous_operations = $2, {}
        ",
        field_spec, parameter_spec, update_spec
    )
}

pub async fn get_bookmarks(pool: &Pool) -> Result<Vec<Bookmark>> {
    let bookmarks = query_as::<_, Bookmark>(
        "
        SELECT
            document,
            previous_operations,
            created,
            url,
            title
        FROM
            `bookmarks-0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b`
        ",
    )
    .fetch_all(pool)
    .await?;

    Ok(bookmarks)
}

pub async fn write_document(pool: &Pool, document: &Document) -> Result<()> {
    // Build query without any bound values
    let query_string = build_insert_query(document);
    let mut query = query(&query_string[..]);

    // Bind values for schema-independent columns
    query = query.bind(document.id().as_str());

    // Create `previous_operations` from comma-separated graph tip hashes
    let graph_tips_param = document
        .current_graph_tips()
        .iter()
        .map(|hash| hash.as_str())
        .collect::<Vec<&str>>()
        .join(",");
    query = query.bind(graph_tips_param);

    // Bind values for schema-specific columns
    match document.view() {
        DocumentView::Available(fields) => {
            for (key, value) in fields.iter() {
                let string_value = match value {
                    // OperationValue::Boolean(value) => value,
                    // OperationValue::Integer(value) => value,
                    // OperationValue::Float(value) => value,
                    OperationValue::Text(value) => value,
                    OperationValue::Relation(value) => value.as_str(),
                    _ => todo!("Oh no it's not a texty thing"),
                };
                log::debug!("Now binding value '{}' = '{}'", key, string_value);
                query = query.bind(string_value);
            }
        },
        DocumentView::Deleted(fields) => {
            log::debug!("Setting all values to '' for deleted document");
            for _ in fields.iter() {
                query = query.bind("");
            }
        }
    };

    // Exectute query
    match query.execute(pool).await {
        Ok(result) => {
            log::info!("Successfully stored materialised view in db: {:?}", result);
            Ok(())
        }
        Err(error) => {
            log::error!("Error storing document in db: {:?}", error);
            Err(error)
        }
    }?;
    Ok(())
}
