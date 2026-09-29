#[cfg(feature = "mongodb")]
mod tests {
    use mongodb::bson::doc;
    use mongodb::{Client, options::ClientOptions};
    use noida::mongodb::server::spawn;
    use std::time::Duration;

    #[tokio::test]
    async fn test_handshake() {
        let addr = spawn("127.0.0.1:0").unwrap();

        let mut client_options =
            ClientOptions::parse(format!("mongodb://{}/?directConnection=true", addr))
                .await
                .unwrap();
        client_options.server_selection_timeout = Some(Duration::from_secs(2));

        let client = Client::with_options(client_options).unwrap();
        let db = client.database("admin");

        // ping
        let ping_res = db.run_command(doc! {"ping": 1}).await;
        assert!(ping_res.is_ok(), "Ping failed: {:?}", ping_res.err());

        // buildInfo
        let build_info = db.run_command(doc! {"buildInfo": 1}).await;
        assert!(build_info.is_ok());
    }

    #[tokio::test]
    async fn test_crud() {
        let addr = spawn("127.0.0.1:0").unwrap();

        let mut client_options =
            ClientOptions::parse(format!("mongodb://{}/?directConnection=true", addr))
                .await
                .unwrap();
        client_options.server_selection_timeout = Some(Duration::from_secs(2));

        let client = Client::with_options(client_options).unwrap();
        let db = client.database("test");

        // Insert
        let insert_res = db
            .run_command(doc! { "insert": "crud", "documents": [doc! {"a": 1, "b": "hello"}]})
            .await
            .unwrap();
        assert_eq!(insert_res.get_i32("n").unwrap(), 1);

        // Find
        let find_res =
            db.run_command(doc! { "find": "crud", "filter": doc! {"a": 1}}).await.unwrap();
        let cursor = find_res.get_document("cursor").unwrap();
        let first_batch = cursor.get_array("firstBatch").unwrap();
        assert_eq!(first_batch.len(), 1);
        let doc = first_batch[0].as_document().unwrap();
        assert_eq!(doc.get_i32("a").unwrap(), 1);
        assert_eq!(doc.get_str("b").unwrap(), "hello");

        // Update
        let update_res = db.run_command(doc! { "update": "crud", "updates": [doc! {"q": {"a": 1}, "u": {"$set": {"a": 2}}}]}).await.unwrap();
        assert_eq!(update_res.get_i32("n").unwrap(), 1);
        assert_eq!(update_res.get_i32("nModified").unwrap(), 1);

        // Find again
        let find_res =
            db.run_command(doc! { "find": "crud", "filter": doc! {"a": 2}}).await.unwrap();
        let cursor = find_res.get_document("cursor").unwrap();
        let first_batch = cursor.get_array("firstBatch").unwrap();
        assert_eq!(first_batch.len(), 1);
        let doc = first_batch[0].as_document().unwrap();
        assert_eq!(doc.get_i32("a").unwrap(), 2);

        // Delete
        let delete_res = db
            .run_command(doc! { "delete": "crud", "deletes": [doc! {"q": {"a": 2}, "limit": 1}]})
            .await
            .unwrap();
        assert_eq!(delete_res.get_i32("n").unwrap(), 1);
    }

    #[tokio::test]
    async fn test_unique_indexes() {
        let addr = spawn("127.0.0.1:0").unwrap();

        let mut client_options =
            ClientOptions::parse(format!("mongodb://{}/?directConnection=true", addr))
                .await
                .unwrap();
        client_options.server_selection_timeout = Some(Duration::from_secs(2));

        let client = Client::with_options(client_options).unwrap();
        let db = client.database("test");

        // Create unique index
        db.run_command(doc! { "createIndexes": "uniq", "indexes": [doc! {"key": {"email": 1}, "name": "email_1", "unique": true}]}).await.unwrap();

        // Insert one doc
        db.run_command(doc! { "insert": "uniq", "documents": [doc! {"email": "x"}]}).await.unwrap();

        // Insert duplicate - should fail
        let dup_res = db
            .run_command(doc! { "insert": "uniq", "documents": [doc! {"email": "x"}]})
            .await
            .unwrap();
        assert_eq!(dup_res.get_f64("ok").unwrap(), 1.0);
        let write_errors = dup_res.get_array("writeErrors").unwrap();
        assert_eq!(write_errors.len(), 1);
        assert_eq!(write_errors[0].as_document().unwrap().get_i32("code").unwrap(), 11000);
    }
}
