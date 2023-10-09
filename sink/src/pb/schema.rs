// @generated
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EntryAdded {
    /// {block-number}-{tx-hash}-{log-index}
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub index: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub uri: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub author: ::prost::alloc::string::String,
    #[prost(string, tag = "5")]
    pub space: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EntriesAdded {
    #[prost(message, repeated, tag = "1")]
    pub entries: ::prost::alloc::vec::Vec<EntryAdded>,
}
// @@protoc_insertion_point(module)
