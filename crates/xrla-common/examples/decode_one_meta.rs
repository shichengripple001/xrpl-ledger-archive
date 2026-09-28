use xrla_common::meta_decode::{account_id_to_classic_address, accounts_touched_by_meta};

fn main() {
    let meta_hex = "201c00000055f8e511006125064667f255752d2c155097c65b120700d836c5f4022b75159d592a0bc8703f4a4011d2f91c56391b797eb4a6706c6a731a9e4f746352060f3d50be937e405a0ee249128a6e43e62405dd2ce06240000000013b1677e1e722000000002405dd2ce12d000000006240000000013b166a811446ffd9dcec08ebbdbc15c7e0adf4198f4bff7fd6e1e1e511006125064667f155012da7d2bb156f4c268a1086f31f1e69fc6cb1da37244c68936ba32ce471436356a0ec72be59fbd9496b52cc605c2af24db0494545fdb0fc4febbfeb18009ea333e66240000018dedc3cede1e7220000000024064640862d000000006240000018dedc3cef8114bba5cc8bd94818feec382b3447d9394d87abe2ace1e1f1031000";
    let meta = hex::decode(meta_hex).unwrap();
    let accounts = accounts_touched_by_meta(&meta).unwrap();
    for a in &accounts {
        println!("{}  ({})", account_id_to_classic_address(a), hex::encode(a));
    }
}
