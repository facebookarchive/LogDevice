---
id: Consensus
title: Distributed consensus
sidebar_label: Distributed consensus
---


The log data model of LogDevice is built on a strongly consistent Paxos consensus engine. The team selected variants of Paxos to achieve:
* fault tolerance with fewer copies.
* flexible quorums for highly available, high throughput, and low latency steady-state replication.
* zero-copy quorum reconfiguration with high availability.

Please see the attached slides for a deep dive into the distributed consensus protocol in LogDevice.

We intend to provide a more in-depth technical document at a later date.

<object data="assets/LogDevice_Consensus_deepdive.pdf" type="application/pdf" width="1000px" height="600px" allowfullscreen>
    <embed src="assets/LogDevice_Consensus_deepdive.pdf" width="1000" height="600" frameborder="0" allowfullscreen>
        <p>This browser does not support PDFs. Please download the PDF to view it: <a href="assets/LogDevice_Consensus_deepdive.pdf">Download PDF</a>.</p>
    </embed>
</object>

<p>
<p><a href="assets/LogDevice_Consensus_deepdive.pdf">Click to download the PDF</a>.</p>
