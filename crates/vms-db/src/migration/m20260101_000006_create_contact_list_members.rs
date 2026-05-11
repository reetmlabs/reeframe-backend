use sea_orm_migration::prelude::*;

use super::m20260101_000004_create_contacts::Contact;
use super::m20260101_000005_create_contact_lists::ContactList;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(ContactListMember::Table)
                    .if_not_exists()
                    .col(ColumnDef::new(ContactListMember::ContactListId).uuid().not_null())
                    .col(ColumnDef::new(ContactListMember::ContactId).uuid().not_null())
                    .primary_key(
                        Index::create()
                            .col(ContactListMember::ContactListId)
                            .col(ContactListMember::ContactId),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_contact_list_members_list")
                            .from(ContactListMember::Table, ContactListMember::ContactListId)
                            .to(ContactList::Table, ContactList::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_contact_list_members_contact")
                            .from(ContactListMember::Table, ContactListMember::ContactId)
                            .to(Contact::Table, Contact::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(ContactListMember::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
pub enum ContactListMember {
    Table,
    ContactListId,
    ContactId,
}
