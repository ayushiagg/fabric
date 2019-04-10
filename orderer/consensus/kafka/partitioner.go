/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package kafka

import "github.com/Shopify/sarama"

type staticPartitioner struct {
	partitionID int32
}

// newStaticPartitioner returns a PartitionerConstructor that
// returns a Partitioner that always chooses the specified partition.
func newStaticPartitioner(partition int32) sarama.PartitionerConstructor {
	logger.Info("KAFKA: file is partitioner.go , func is  newStaticPartitioner()")
	return func(topic string) sarama.Partitioner {
		return &staticPartitioner{partition}
	}
}

// Partition takes a message and partition count and chooses a partition.
func (prt *staticPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	logger.Info("KAFKA: file is partitioner.go , func is  Partition()")
	return prt.partitionID, nil
}

// RequiresConsistency indicates to the user of the partitioner whether the
// mapping of key->partition is consistent or not.
func (prt *staticPartitioner) RequiresConsistency() bool {
	logger.Info("KAFKA: file is partitioner.go , func is  RequiresConsistency()")
	return true
}
