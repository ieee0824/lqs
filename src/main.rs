use lqs::{ServerConfig, serve};

#[tokio::main]
async fn main() {
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
    eprintln!("SSE/KMS settings are configuration-only; SQLite payloads remain plaintext.");
    if let Err(error) = serve(config).await {
        eprintln!("LQS server failed: {error}");
        std::process::exit(1);
    }
}
