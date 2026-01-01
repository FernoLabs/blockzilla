# Blockzilla

tar cz --no-xattrs --exclude target --exclude .git --exclude epochs --exclude blockzilla-v1 . | ssh ach@blockzilla.local 'mkdir -p ~/dev/blockzilla && tar xz -C ~/dev/blockzilla-v1'

tar cz --no-xattrs --exclude target --exclude .git --exclude epochs --exclude blockzilla-v1 . | ssh ach@192.168.1.45 -p 22 'mkdir -p ~/dev/blockzilla && tar xz -C ~/dev/blockzilla-v1'

```
# allow perf for normal users (common dev setting)
 sudo sysctl -w kernel.perf_event_paranoid=1
cargo flamegraph --profile release-debug --bin reader --features="reader" --  --decode-tx ./epochs/epoch-0.car.zst
```

cargo run --profile release-debug --bin reader --features="reader" --  --decode-tx ./epochs/epoch-0.car.zst

## mac build

```
brew install gcc
export CC=/opt/homebrew/bin/gcc-15                                                                                       
export CXX=/opt/homebrew/bin/g++-15
export CXXFLAGS="-std=c++11"
```


LC_ALL=C sort -T tmp … | uniq -c | LC_ALL=C sort -T tmp -nr


gh repo list solana-program --limit 4000 | while read -r repo _; do
  gh repo clone "$repo" "$repo"
done