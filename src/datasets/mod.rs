pub mod entities;

pub struct Dataset<I> {
    // pattern to match files
    filepattern: String,

    // deserializer for files
    deser: fn(String) -> I,
}

// impl<I> Dataset<I> {
//     fn
// }
