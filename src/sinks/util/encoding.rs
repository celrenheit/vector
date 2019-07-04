use crate::event;

pub fn encode_logfmt(iter: &mut event::FieldsIter) -> Vec<u8> {
    let mut elements = Vec::new();
    for (k, v) in iter {
        let e = format!("{}={:?}", k, v.to_string_lossy());
        elements.push(e);
    }

    elements.join(" ").as_bytes().to_vec()
}
