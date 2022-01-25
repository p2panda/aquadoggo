use p2panda_rs::hash::Hash;
use p2panda_rs::instance::Instance;
use p2panda_rs::operation::OperationValue;
use sqlx::{query, query_as, FromRow};

use crate::db::Pool;
use crate::errors::Result;

#[derive(FromRow)]
pub struct Bookmark {
    pub document: String,
    pub created: String,
    pub url: String,
    pub title: String,
}

fn build_insert_query(instance: &Instance) -> String {
    // The field spec enumerates all schema-specific vield names
    let field_spec = instance
        .iter()
        .map(|(fieldname, _)| fieldname.clone())
        .reduce(|acc, val| format!("{}, {}", acc, val))
        .unwrap();

    // The parameter spec is a list of parameter placeholders with as many elements as there are
    // columns to insert
    let parameter_spec = (0..instance.raw().len())
        .map(|i| format!("${}", (i + 2)))
        .reduce(|acc, elem| format!("{}, {}", acc, elem))
        .unwrap();

    format!(
        "
        INSERT INTO
            `bookmarks-0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b`
            (document, {})
        VALUES
            ($1, {})
        ON CONFLICT (document) DO UPDATE SET
            created=$2,
            title=$3,
            url=$4,
        ",
        field_spec, parameter_spec
    )
}

pub async fn get_bookmarks(pool: &Pool) -> Result<Vec<Bookmark>> {
    let bookmarks = query_as::<_, Bookmark>(
        "
        SELECT
            document,
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

pub async fn write_document(pool: &Pool, document_id: &Hash, instance: &Instance) -> Result<()> {
    // Build query without any bound values
    let query_string = build_insert_query(instance);
    let mut query = query(&query_string[..]);

    // Bind values for schema-independent columns
    query = query.bind(document_id);

    // Bind values for schema-specific columns
    for (_, value) in instance.raw() {
        let string_value = match value {
            // OperationValue::Boolean(value) => value,
            // OperationValue::Integer(value) => value,
            // OperationValue::Float(value) => value,
            OperationValue::Text(value) => value,
            OperationValue::Relation(value) => value.as_str().into(),
            _ => todo!("Oh no it's not a texty thing"),
        };
        query = query.bind(string_value);
    }

    // Exectute query
    query.execute(pool).await?;
    Ok(())
}
