//! `SeaORM` Entity. Generated by sea-orm-codegen 0.12.2

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "entities")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub id: String,
    pub name: Option<String>,
    pub is_type: Option<bool>,
    #[sea_orm(column_type = "Text", nullable)]
    pub defined_in: Option<String>,
    #[sea_orm(column_type = "Text", nullable)]
    pub value_type: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::spaces::Entity",
        from = "Column::DefinedIn",
        to = "super::spaces::Column::Address"
    )]
    Spaces,
}

impl Related<super::spaces::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Spaces.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
