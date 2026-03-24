dvc-s3
======

s3 plugin for dvc

This fork replaces the default ``s3fs`` transfer backend with
``s3transfer`` — the same engine behind the AWS CLI — providing parallel
byte-range GETs (downloads) and optimized multipart uploads for
significantly faster ``dvc push`` / ``dvc pull``.

Installation
------------

Install from this repository (replaces the official ``dvc-s3`` if already
installed):

.. code-block:: bash

   pip install git+https://github.com/justln1113/dvc-s3.git

If you previously installed the official version, the command above will
automatically replace it. To verify:

.. code-block:: bash

   pip show dvc-s3          # Source should point to this repo
   dvc version              # Should list dvc-s3 under Subprojects

To revert to the official version at any time:

.. code-block:: bash

   pip install dvc-s3       # Reinstalls from PyPI

Quick Start
-----------

After installation, apply the recommended settings for your remote and
you are ready to go:

.. code-block:: bash

   dvc remote modify myremote max_concurrent_requests 20
   dvc remote modify myremote multipart_threshold 8MB
   dvc remote modify myremote multipart_chunksize 8MB
   dvc remote modify myremote jobs 32

Replace ``myremote`` with the name of your S3 remote
(run ``dvc remote list`` to check).

Performance Tuning
------------------

All parameters can be set via ``dvc remote modify``:

.. code-block:: bash

   dvc remote modify myremote <parameter> <value>

Transfer Parameters
~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 30 15 55

   * - Parameter
     - Default
     - Description
   * - ``max_concurrent_requests``
     - ``20``
     - Number of parallel threads for multipart upload parts and byte-range
       GET download chunks **per file**. Higher values improve single large
       file speed at the cost of memory and bandwidth.
   * - ``multipart_threshold``
     - ``8MB``
     - Files larger than this are transferred using multipart (parallel
       parts). Files below this threshold use a single PUT/GET request.
       Accepts human-readable sizes (e.g. ``8MB``, ``16MiB``).
   * - ``multipart_chunksize``
     - ``8MB``
     - Size of each part in a multipart transfer. Smaller chunks yield more
       parallelism but more S3 API calls. Minimum is ``5MB`` (S3 limit).
       Accepts human-readable sizes.
   * - ``max_queue_size``
     - ``100``
     - Maximum number of queued I/O operations in the transfer manager.
   * - ``jobs``
     - ``4 × CPU cores``
     - Number of **files** transferred in parallel (DVC-level). This is
       independent from ``max_concurrent_requests`` which controls
       parallelism **within** each file.

Recommended Settings for Audio / Annotation Workloads
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you primarily transfer **WAV/FLAC audio files** alongside small
**TextGrid or other annotation files**, the following settings are tuned
to approximate rclone-level throughput:

.. code-block:: bash

   # Per-file parallelism — raise for large WAV files
   dvc remote modify myremote max_concurrent_requests 20

   # Start multipart at 8MB — most FLAC files and all WAV > 8MB benefit
   dvc remote modify myremote multipart_threshold 8MB

   # 8MB chunks — good balance between parallelism and API call overhead
   dvc remote modify myremote multipart_chunksize 8MB

   # File-level parallelism — raise for many small TextGrid files
   dvc remote modify myremote jobs 32

**Why these values work:**

- **Small files (TextGrid, short FLAC < 8 MB)** stay below the multipart
  threshold and are uploaded/downloaded in a single request. Speed comes
  from ``jobs`` pushing many files concurrently.
- **Large files (WAV, long FLAC > 8 MB)** are split into 8 MB chunks and
  transferred with up to 20 parallel threads each, saturating the network
  link much like rclone.

For very large WAV files (> 500 MB) or high-bandwidth links (> 1 Gbps),
consider raising concurrency further:

.. code-block:: bash

   dvc remote modify myremote max_concurrent_requests 50
   dvc remote modify myremote multipart_chunksize 16MB
   dvc remote modify myremote jobs 64

rclone Comparison
~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 35 35 30

   * - Concept
     - rclone flag
     - dvc-s3 equivalent
   * - Files in parallel
     - ``--transfers 4``
     - ``jobs`` (default: 4 × CPU cores)
   * - Parts per file
     - ``--s3-upload-concurrency 4``
     - ``max_concurrent_requests`` (default: 20)
   * - Multipart chunk size
     - ``--s3-chunk-size 5M``
     - ``multipart_chunksize`` (default: 8MB)

Tests
-----

By default tests will be run against moto.
To run against real S3, set ``DVC_TEST_AWS_REPO_BUCKET`` with an AWS bucket name.
