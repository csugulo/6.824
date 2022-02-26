#/bin/bash

./bin/mrsequential lib/wc.so res/*.txt || exit 1
sort mr-out-0 > mr-correct-wc.txt
rm mr-out*


echo '***' Starting wc test.

timeout -k 2s 180s ./bin/coordinator lib/wc.so 10 res/*.txt &
sleep 1

timeout -k 2s 180s ./bin/worker &
timeout -k 2s 180s ./bin/worker &
timeout -k 2s 180s ./bin/worker &

wait

sort mr-out* | grep . > mr-wc-all
rm mr-out*
if cmp mr-wc-all mr-correct-wc.txt
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
  failed_any=1
fi

rm -f mr-*