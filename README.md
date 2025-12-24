# Blockzilla

tar cz --no-xattrs --exclude target --exclude .git --exclude epochs --exclude optimized . | ssh ach@blockzilla.local 'mkdir -p ~/dev/blockzilla && tar xz -C ~/dev/blockzilla-v1'

cargo run --release -- profile -i path/to/archive.car.zst --seconds 60 --out flamegraph.svg


## mac build

```
brew install gcc
export CC=/opt/homebrew/bin/gcc-15                                                                                       
export CXX=/opt/homebrew/bin/g++-15
export CXXFLAGS="-std=c++11"
```