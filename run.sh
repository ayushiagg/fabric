docker system prune --all --force --volumes

make clean-all clean

make dist-clean all release docker

cp -r release/linux-amd64/bin/* ../fabric-samples/bin/.

cd orderer/consensus/elastico/consumer/

go build consumer.go

cp consumer ../../../../../fabric-samples/bin/.

cd -

cd ../fabric-samples/first-network

./byfn.sh generate

./byfn.sh up

