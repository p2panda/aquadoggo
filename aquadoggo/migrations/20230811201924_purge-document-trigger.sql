-- SPDX-License-Identifier: AGPL-3.0-or-later

CREATE TRIGGER purge_document 
   AFTER DELETE ON documents
BEGIN
    DELETE FROM logs
    WHERE logs.document = OLD.document_id;
    
    DELETE FROM entries 
    WHERE entries.entry_hash IN (
        SELECT operations_v1.operation_id FROM operations_v1
        WHERE operations_v1.document_id = OLD.document_id
    );
    
    DELETE FROM operations_v1
    WHERE operations_v1.document_id = OLD.document_id;
END;
