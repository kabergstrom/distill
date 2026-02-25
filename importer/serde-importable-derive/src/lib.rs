extern crate proc_macro;

use quote::{quote, ToTokens};

#[proc_macro_derive(SerdeImportable, attributes(uuid))]
pub fn serde_importable_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let ast: syn::DeriveInput = syn::parse(input).unwrap();

    // Build the trait implementation
    let name = &ast.ident;

    let mut uuid = None;
    for attr in &ast.attrs {
        let syn::Meta::NameValue(name_value) = &attr.meta else {
            continue;
        };

        if !name_value.path.is_ident("uuid") {
            continue;
        }

        let uuid_str: syn::LitStr = syn::parse2(name_value.value.to_token_stream())
            .expect("uuid attribute must take the form `#[uuid = \"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx\"]`");

        uuid = Some(uuid_str);
    }

    let uuid =
        uuid.expect("No `#[uuid = \"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx\"]` attribute found");
    let gen = quote! {
        #[distill_importer::typetag::serde(name = #uuid)]
        impl distill_importer::SerdeImportable for #name {
        }
    };
    gen.into()
}
