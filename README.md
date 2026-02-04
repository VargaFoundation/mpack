# Varga Management Pack (Kirka & Tarn)

This project contains a Management Pack (mpack) for Apache Ambari to deploy and manage Kirka and Tarn services.

## Project Structure

- `mpack-varga/`: Management Pack source code.
- `build/`: Build scripts.

## Build

To build the mpack, run the following script from the `build` directory:

```bash
cd build
chmod +x build_mpack.sh
./build_mpack.sh
```

The generated file will be located at `build/target/varga-mpack-1.0.0.0.tar.gz`.

## Configuration

The services are configurable via the Ambari Web UI. Key configuration categories include:
- `kirka-site`: Kirka server port, installation directory, security (Kerberos/Ranger), and HDFS/HBase integration.
- `tarn-site`: Tarn server port, installation directory, YARN/Prometheus integration, and scaling policies.

## Installation in Ambari

To install the mpack on your Ambari server:

```bash
ambari-server install-mpack --mpack=/path/to/varga-mpack-1.0.0.0.tar.gz --verbose
ambari-server restart
```