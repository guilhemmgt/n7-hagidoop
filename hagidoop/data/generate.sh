cp $1 /work/tmp/data.txt

# generate 2^$2 times file $1
n=1
while [ "$n" -le $2 ]; do
echo "$n"
cat /work/tmp/data.txt /work/tmp/data.txt > /work/tmp/temp
mv /work/tmp/temp /work/tmp/data.txt
n=$(( n + 1 ))
done
mv /work/tmp/data.txt /work
