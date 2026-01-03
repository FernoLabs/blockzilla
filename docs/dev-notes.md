## Deploy to local node
 
 ```
tar cz --no-xattrs --exclude target --exclude .git --exclude epochs --exclude blockzilla-v1 . | ssh ach@blockzilla.local 'mkdir -p ~/dev/blockzilla && tar xz -C ~/dev/blockzilla-v1'
```

## Under VPN without local DNS

```
tar cz --no-xattrs --exclude target --exclude .git --exclude epochs --exclude blockzilla-v1 . | ssh ach@192.168.1.45 -p 22 'mkdir -p ~/dev/blockzilla && tar xz -C ~/dev/blockzilla-v1'
```

## Benchmark

```
# allow perf for normal users (common dev setting)
sudo sysctl -w kernel.perf_event_paranoid=1

cargo flamegraph --profile release-debug --bin reader --features="reader" --  --decode-tx ./epochs/epoch-0.car.zst
```

## Strings analysys

```
cargo run --release --bin blockzilla dump-log-strings --input blockzilla-v1/epoch-800/compact.bin --out dumps-800.log
# need a tmp folder as /tmp may be to small
rm -r tmp && mkdir tmp
LC_ALL=C sort -T tmp dumps-800.log | uniq -c | LC_ALL=C sort -T tmp -nr
```

## mac build

```
brew install gcc
export CC=/opt/homebrew/bin/gcc-15                                                                                       
export CXX=/opt/homebrew/bin/g++-15
export CXXFLAGS="-std=c++11"
```