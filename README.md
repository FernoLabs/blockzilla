# Blockzilla

This repo contains tool use to build and maitain ferno archive node

cargo run --release --bin blockzilla network \
  --source https://files.old-faithful.net/0/epoch-0.car  \
  --output-dir optimized/

## archive builder

```bash
# download epoch car file from old-faithful archive or IPFS
curl -fSL https://files.old-faithful.net/0/epoch-0.car -o epoch-0.car # /!\ epoch 0 is 4Gb

# run optimizer
cargo run --release optimize --file epoch-0.car

# get block out optimized archive
cargo run --release -- read --epoch "optimized/epoch-0.bin" --idx "optimized/epoch-0.idx" --registry "optimized/registry.sqlite" 4  
```

## Deploy to local blockzilla

```bash
tar cz --no-xattrs --exclude target --exclude .git --exclude epoch-0.car --exclude epoch-1.car --exclude optimized . | ssh ach@blockzilla.local 'mkdir -p ~/dev/blockzilla && tar xz -C ~/dev/blockzilla'
```

## mac m1 build

gcc is required to build protobuff related package

```bash
brew install gcc
export CC=/opt/homebrew/bin/gcc-15                                                                                       
export CXX=/opt/homebrew/bin/g++-15
export CXXFLAGS="-std=c++11"
```