Build:
  make

Run (3-node mesh, localhost):
  ./auction A 9000 9001 9002 &
  ./auction B 9001 9000 9002 &
  ./auction C 9002 9000 9001 &

Talk to it (interactive, recommended):
  nc localhost 9000
    REGISTER alice
    BID 500
    STATUS

Talk to it (piped -- keep stdin open past EOF or nc shows nothing):
  (printf 'REGISTER alice\nBID 500\nSTATUS\n'; sleep 0.3) | nc localhost 9000

Close the auction (this node only, broadcasts to its clients):
  (printf 'CLOSE\n'; sleep 0.3) | nc localhost 9000

Test:
  bash test/test.sh
