use arbitrary::Arbitrary;

bitflags::bitflags! {
    pub struct MessageFlags: u32 {
         const CHECKSUM_PRESENT = 0b_0000_0000_0000_0000_0000_0000_0000_0001;
         const MORE_TO_COME     = 0b_0000_0000_0000_0000_0000_0000_0000_0010;
         const EXHAUST_ALLOWED  = 0b_0000_0000_0000_0001_0000_0000_0000_0000;
    }
}

impl<'a> Arbitrary<'a> for MessageFlags {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let bits = u.arbitrary::<u32>()?;
        Ok(MessageFlags::from_bits(bits).unwrap_or(MessageFlags::empty()))
    }
}
