BEGIN=250
END=100000
STEP=500
ITER=50
cargo run --release  -- -s $STEP -b $BEGIN -e $END  -i $ITER -c mixed-tuple
cargo run --release  -- -s $STEP -b $BEGIN -e $END  -i $ITER -c utf8-tuple
cargo run --release  -- -s $STEP -b $BEGIN -e $END  -i $ITER -c dictionary-tuple
cargo run --release  -- -s $STEP -b $BEGIN -e $END  -i $ITER -c mixed-dictionary-tuple
