use serde::ser::{Serialize, Serializer};

use ::Index;

#[derive(Debug)]
pub struct Point {
    pub kind: &'static str,
    pub value: f64,
}

impl Serialize for Point {
    fn serialize<S>(&self, serializer: &mut S) -> ::std::result::Result<(), S::Error>
        where S: Serializer
    {
        let mut state = serializer.serialize_map(Some(2))?;
        serializer.serialize_map_key(&mut state, "pointType")?;
        serializer.serialize_map_value(&mut state, self.kind)?;
        serializer.serialize_map_key(&mut state, "value")?;
        serializer.serialize_map_value(&mut state, self.value)?;
        serializer.serialize_map_end(state)
    }
}
#[derive(Debug)]
pub struct Entry {
    pub index: Index,
    /// limit to 4 element
    pub time: u64,
    pub points: Vec<Point>,
}


impl Serialize for Entry {
    fn serialize<S>(&self, s: &mut S) -> ::std::result::Result<(), S::Error>
        where S: Serializer
    {
        let mut state = s.serialize_map(Some(4))?;

        // 1. Metric
        s.serialize_map_key(&mut state, "name")?;
        s.serialize_map_value(&mut state, &self.index.metric)?;

        // 2. tags
        s.serialize_map_key(&mut state, "tags")?;
        // tags values;
        let len = self.index.tags.len();
        let mut tag_state = s.serialize_map(Some(len))?;
        for kv in &self.index.tags {
            let ksp: Vec<_> = kv.splitn(2, "=").collect();
            if ksp.len() != 2 {
                continue;
            }

            s.serialize_map_key(&mut tag_state, ksp[0])?;
            s.serialize_map_value(&mut tag_state, ksp[1])?;
        }
        s.serialize_map_end(tag_state)?;

        // 3. time
        s.serialize_map_key(&mut state, "timestamp")?;
        s.serialize_map_value(&mut state, self.time)?;

        // 4. points
        s.serialize_map_key(&mut state, "points")?;
        s.serialize_map_value(&mut state, &self.points)?;

        s.serialize_map_end(state)
    }
}

#[cfg(test)]
mod bench {
    use super::*;
    use test::Bencher;
    use std::collections::BTreeSet;
    use ::com;
    use ::Index;
    use serde_json;

    pub fn product() -> Entry {
        let mut tags = BTreeSet::new();
        tags.insert("from=localhost".to_owned());
        tags.insert("service=lin.rs".to_owned());
        tags.insert("iface=encode".to_owned());
        let idx = Index::build("helloWorld".to_string(), tags, 'c').unwrap();
        let vecs = vec![
            Point{kind: "SUM", value: 50000.0},
            Point{kind: "COUNT", value:11.0},
            Point{kind: "MAX", value:2013.0},
            Point{kind: "MIN", value:10.0},];
        let now = com::now();
        Entry {
            index: idx,
            points: vecs,
            time: now,
        }
    }

    pub fn encode(entry: &Entry) -> usize {
        let encoded = serde_json::to_string(&entry).unwrap();
        encoded.len()
    }

    #[bench]
    fn bench_encode(b: &mut Bencher) {
        let entry = product();
        let mut len = 0usize;
        let mut count = 0usize;
        b.iter(|| for _ in 0..10000 {
            len += encode(&entry);
            count += 1;
        });
        let len = len as f64 / 1024.0 / 1024.0;
        let _avg = len / (count as f64);
    }
}
