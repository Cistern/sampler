package main

import (
	"bytes"
	"flag"
	"log"
	"net"
	"syscall"

	"github.com/PreetamJinka/sflow"
	"github.com/PreetamJinka/udpchan"
)

// host order (usually little endian) -> network order (big endian)
func htons(n int) int {
	return int(int16(byte(n))<<8 | int16(byte(n>>8)))
}

func main() {
	sampleEvery := flag.Int("sample-every", 256, "sample every N packets")
	sampleSize := flag.Int("sample-size", 256, "sample size in bytes")
	collectorAddress := flag.String("collector-address", ":6343", "address of collector")
	sourceIP := flag.String("source-ip", "127.0.0.1", "IP address of this host")
	flag.Parse()

	log.Println("Starting sampler")

	fd, err := syscall.Socket(syscall.AF_PACKET, syscall.SOCK_RAW, htons(syscall.ETH_P_ALL))
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Opened a raw socket")

	collector, err := udpchan.Connect(*collectorAddress)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Ready to send to collector at", *collectorAddress)

	enc := sflow.NewEncoder(net.ParseIP(*sourceIP), 1, 1)

	i := 0
	sequenceNum := 0
	buf := make([]byte, 65535)
	outboundBuf := &bytes.Buffer{}
	for {
		i++

		n, _, err := syscall.Recvfrom(fd, buf, 0)
		if err != nil {
			log.Fatal(err)
		}

		if i%*sampleEvery != 0 {
			continue
		}

		numStripped := 0
		if n > *sampleSize {
			numStripped = n - *sampleSize
		}

		flowSample := &sflow.FlowSample{
			SequenceNum:  uint32(sequenceNum),
			SamplingRate: uint32(*sampleEvery),
			Records: []sflow.Record{
				sflow.RawPacketFlow{
					Protocol:    1, // 1 = Ethernet
					FrameLength: uint32(n),
					Stripped:    uint32(numStripped),
					HeaderSize:  uint32(n - numStripped),
					Header:      buf[:n-numStripped],
				},
			},
		}

		err = enc.Encode(outboundBuf, []sflow.Sample{flowSample})
		if err != nil {
			log.Fatal(err)
		}

		collector <- outboundBuf.Bytes()

		log.Println("Sent a sample")

		sequenceNum++

		outboundBuf.Reset()
	}

	log.Println("sampler exiting")
}
