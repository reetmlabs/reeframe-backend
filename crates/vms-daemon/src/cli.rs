//! CLI subcommands that run instead of starting the daemon.
//!
//! `vms-daemon` with no arguments starts the daemon. `vms-daemon token ...`
//! connects to the configured database, mints, lists or revokes an
//! [`vms_db::ApiKeyRepo`] token, and exits without starting media, pipelines
//! or the HTTP server. This lets an operator provision a credential for the
//! FE (or any other client) from a shell on the host, without logging in
//! through the HTTP API first.

use clap::{Args, Parser, Subcommand};
use sea_orm::Database;
use uuid::Uuid;
use vms_db::{entities::user, repos::api_key::CreateApiKey, ApiKeyRepo, UserRepo};

use crate::config;

#[derive(Parser)]
#[command(name = "vms-daemon", version, about = "Reeframe VMS daemon")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Subcommand)]
pub enum Command {
    /// Mint, list, or revoke long-lived API tokens (e.g. for the FE) without starting the daemon.
    Token {
        #[command(subcommand)]
        action: TokenAction,
    },
}

#[derive(Subcommand)]
pub enum TokenAction {
    /// Mint a new token and print the raw value once.
    Generate(TokenGenerateArgs),
    /// List token metadata (id, name, created_at, last_used). The raw value
    /// is never stored anywhere after creation, so it can't be shown again.
    List(TokenListArgs),
    /// Revoke a token by id.
    Revoke { key_id: Uuid },
}

#[derive(Args)]
pub struct TokenGenerateArgs {
    /// User to mint the token for. Required unless exactly one user exists.
    #[arg(long)]
    pub username: Option<String>,
    /// Label stored alongside the token, shown by `token list`.
    #[arg(long, default_value = "cli")]
    pub name: String,
}

#[derive(Args)]
pub struct TokenListArgs {
    /// Restrict the listing to one user. Lists every user's tokens if omitted.
    #[arg(long)]
    pub username: Option<String>,
}

pub async fn run(command: Command) -> anyhow::Result<()> {
    let cfg = config::load().map_err(|e| anyhow::anyhow!(e))?;
    let db = Database::connect(&cfg.database.url).await?;
    let user_repo = UserRepo::new(db.clone());
    let api_key_repo = ApiKeyRepo::new(db);

    let Command::Token { action } = command;
    match action {
        TokenAction::Generate(args) => generate(&user_repo, &api_key_repo, args).await,
        TokenAction::List(args) => list(&user_repo, &api_key_repo, args).await,
        TokenAction::Revoke { key_id } => revoke(&api_key_repo, key_id).await,
    }
}

/// Resolves `--username` against the `users` table. Without it, falls back
/// to the only user if exactly one exists, which is the common single-admin
/// deployment.
async fn resolve_user(
    user_repo: &UserRepo,
    username: Option<String>,
) -> anyhow::Result<user::Model> {
    if let Some(username) = username {
        return user_repo
            .get_by_username(&username)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no such user: {username}"));
    }

    let mut users = user_repo.list().await?;
    match users.len() {
        0 => Err(anyhow::anyhow!(
            "no users exist yet, complete first-run setup (POST /auth/setup) before minting a token"
        )),
        1 => Ok(users.remove(0)),
        _ => {
            let names: Vec<&str> = users.iter().map(|u| u.username.as_str()).collect();
            Err(anyhow::anyhow!(
                "multiple users exist, pass --username <name> (one of: {})",
                names.join(", ")
            ))
        }
    }
}

async fn generate(
    user_repo: &UserRepo,
    api_key_repo: &ApiKeyRepo,
    args: TokenGenerateArgs,
) -> anyhow::Result<()> {
    let user = resolve_user(user_repo, args.username).await?;
    let created = api_key_repo
        .create(CreateApiKey {
            user_id: user.id,
            name: args.name,
        })
        .await?;

    eprintln!(
        "Token minted for user '{}' (key id: {}), shown once, store it now. \
         The FE authenticates with it via the `x-api-key` header.",
        user.username, created.model.id
    );
    println!("{}", created.raw_key);
    Ok(())
}

async fn list(
    user_repo: &UserRepo,
    api_key_repo: &ApiKeyRepo,
    args: TokenListArgs,
) -> anyhow::Result<()> {
    let users = if let Some(username) = args.username {
        vec![user_repo
            .get_by_username(&username)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no such user: {username}"))?]
    } else {
        user_repo.list().await?
    };

    let mut printed_any = false;
    for user in users {
        for key in api_key_repo.list_for_user(user.id).await? {
            printed_any = true;
            let last_used = key
                .last_used
                .map(|t| t.to_rfc3339())
                .unwrap_or_else(|| "never".to_string());
            println!(
                "{}  user={}  name={:?}  created_at={}  last_used={}",
                key.id,
                user.username,
                key.name,
                key.created_at.to_rfc3339(),
                last_used,
            );
        }
    }
    if !printed_any {
        eprintln!("No tokens found.");
    }
    Ok(())
}

async fn revoke(api_key_repo: &ApiKeyRepo, key_id: Uuid) -> anyhow::Result<()> {
    api_key_repo.delete_by_id(key_id).await?;
    eprintln!("Token {key_id} revoked.");
    Ok(())
}
