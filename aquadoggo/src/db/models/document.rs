use p2panda_rs::hash::Hash;
use p2panda_rs::instance::Instance;
use sqlx::query;

use crate::db::Pool;
use crate::errors::Result;

const FIELDS: [&str; 4] = ["document", "url", "title", "created"];

fn build_insert_query(document_id: &Hash, instance: &Instance) -> String {
    // The field spec enumerates all schema-specific vield names
    let field_spec = FIELDS
        .iter()
        .map(|elem| elem.to_string())
        .reduce(|acc, val| format!("{}, {}", acc, val))
        .unwrap();

    // The parameter spec is a list of parameter placeholders with as many elements as there are
    // columns to insert
    let parameter_spec = (0..FIELDS.len())
        .map(|i| i.to_string())
        .reduce(|elem, spec| format!("{}, ${}", spec, elem))
        .unwrap();

    format!(
        "
        INSERT INTO
            `bookmarks-0020c65567ae37efea293e34a9c7d13f8f2bf23dbdc3b5c7b9ab46293111c48fc78b`
            ({})
        VALUES
            ({})
        ",
        field_spec, parameter_spec
    )
}

pub async fn write_document(pool: &Pool, document_id: &Hash, instance: &Instance) -> Result<()> {
    // Build query without any bound values
    let query = query(&build_insert_query(document_id, instance)[..]);

    // Bind values for schema-independent columns
    query = query.bind(document_id);

    // Bind values for schema-specific columns
    for field_name in &FIELDS[1..] {
        let value = instance.raw().get(field_name.to_owned()).unwrap();
        query = query.bind(value);
    }

    // Exectute query
    query.execute(pool).await?;
    Ok(())
}
