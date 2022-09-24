# System performance monitor

Docker image with only one task - it collects data from `atop` and present them
in single dashboard - where you can see usage of cpu, memory and disks - and
thus try to find bottlenecks of your software.

It also allows for adding annotations to the dashboard, so you can run some
performance tests and add information about test and its params, or about
particulars parts of it.
