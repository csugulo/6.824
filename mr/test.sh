#/bin/bash

############# wc #############

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

wait ; wait ; wait

rm -f mr-*

############# indexer #############

./bin/mrsequential lib/indexer.so res/*.txt || exit 1
sort mr-out-0 > mr-correct-indexer.txt
rm mr-out*


echo '***' Starting indexer test.

timeout -k 2s 180s ./bin/coordinator lib/indexer.so 10 res/*.txt &
sleep 1

timeout -k 2s 180s ./bin/worker &
timeout -k 2s 180s ./bin/worker &
timeout -k 2s 180s ./bin/worker &

wait

sort mr-out* | grep . > mr-indexer-all
rm mr-out*
if cmp mr-indexer-all mr-correct-indexer.txt
then
  echo '---' indexer test: PASS
else
  echo '---' indexer output is not the same as mr-correct-indexer.txt
  echo '---' indexer test: FAIL
  failed_any=1
fi

wait ; wait ; wait

rm -f mr-*


############# crash #############

./bin/mrsequential lib/nocrash.so res/*.txt || exit 1
sort mr-out-0 > mr-correct-nocrash.txt
rm mr-out*


echo '***' Starting crash test.

timeout -k 2s 180s ./bin/coordinator lib/crash.so 10 res/*.txt &
sleep 1

timeout -k 2s 180s ./bin/worker &
timeout -k 2s 180s ./bin/worker &
timeout -k 2s 180s ./bin/worker &

wait

sort mr-out* | grep . > mr-crash-all
rm mr-out*
if cmp mr-crash-all mr-correct-nocrash.txt
then
  echo '---' crash test: PASS
else
  echo '---' crash output is not the same as mr-correct-nocrash.txt
  echo '---' crash test: FAIL
  failed_any=1
fi

wait ; wait ; wait

rm -f mr-*
