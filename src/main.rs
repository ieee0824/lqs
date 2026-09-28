use lqs::{ServerConfig, migrate_plaintext_database, read_database_key_file, serve};

#[tokio::main]
async fn main() {
    let mut args = std::env::args_os();
    args.next();
    if let Some(command) = args.next() {
        if command != "migrate-plaintext" {
            eprintln!("usage: lqs migrate-plaintext <source-db> <target-db> <key-file>");
            std::process::exit(2);
        }
        let (Some(source), Some(target), Some(key_file), None) =
            (args.next(), args.next(), args.next(), args.next())
        else {
            eprintln!("usage: lqs migrate-plaintext <source-db> <target-db> <key-file>");
            std::process::exit(2);
        };
        let result = read_database_key_file(key_file)
            .and_then(|key| migrate_plaintext_database(source, target, &key));
        if let Err(error) = result {
            eprintln!("database migration failed: {error}");
            std::process::exit(1);
        }
        println!("encrypted database created; plaintext source remains in place");
        return;
    }
    let config = match ServerConfig::from_env() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("invalid server configuration: {error}");
            std::process::exit(2);
        }
    };

    println!(
        "LQS listening on {} (database: {})",
        config.bind_addr,
        config.database_path.display()
    );
    if config.trust_principal_header {
        eprintln!(
            "WARNING: x-lqs-principal is self-asserted and trusted for local simulation only."
        );
    }
    if config.allow_unauthenticated_remote && config.bearer_credential.is_none() {
        eprintln!("WARNING: accepting unauthenticated requests on a non-loopback interface.");
    }
    if config.database_key_file.is_some() {
        eprintln!(
            "SQLCipher storage encryption enabled; SQS SSE/KMS settings remain simulation-only."
        );
    } else {
        eprintln!("SSE/KMS settings are configuration-only; SQLite payloads remain plaintext.");
    }
    if let Err(error) = serve(config).await {
        eprintln!("LQS server failed: {error}");
        std::process::exit(1);
    }
}
