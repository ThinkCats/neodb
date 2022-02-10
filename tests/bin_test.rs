#[test]
fn test_bin_number() {
    let number: i64 = 2036854774807;
    let encode = bincode::serialize(&number).unwrap();
    println!("encode:{:?} ,size:{}", encode, encode.len());
    let result: i64 = bincode::deserialize(&encode[..]).unwrap();
    println!("result is:{}", result);

    let ss = "hello world";
    let encode = bincode::serialize(&ss).unwrap();
    println!("encode:{:?}", encode);
    let result: String = bincode::deserialize(&encode[..]).unwrap();
    println!("result is:{}", result);

    let a = vec![23, 21, 55, 33, 31];
    let b = a.get(0..2).unwrap();
    let c = a.get(2..4).unwrap();
    println!("slice b:{:?},c:{:?}", b, c);

    let v = vec![5, 0, 0, 0, 0, 0, 0, 0, 104, 119, 108, 108, 59];
    let re: String = bincode::deserialize(&v[..]).unwrap();
    println!("slice full:{:?}", re);
}
