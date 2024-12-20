use arbitrary::Arbitrary;

bitflags::bitflags! {
    pub struct MessageFlags: u32 {
        const NONE = 0;
        const CHECKSUM_PRESENT = 0x04;
        const MORE_TO_COME = 0x02;
        const EXHAUST_ALLOWED = 0x10000;
    }
}

impl<'a> Arbitrary<'a> for MessageFlags {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let bits = u.arbitrary::<u32>()?;
        Ok(MessageFlags::from_bits(bits).unwrap_or(MessageFlags::empty()))
    }
}
