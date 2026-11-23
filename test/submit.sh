
#!/usr/bin/env bash
set -euo pipefail

# Delta-to-SQL: cliente Spark temporal desde Docker Desktop

if [[ $# -lt 1 ]]; then
    echo "Uso: $0 <job.py> [argumentos...]"
    exit 1
fi

JOB="$1"
shift

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ "$JOB" != /* ]]; then
    JOB="${PROJECT_DIR}/${JOB}"
fi

if [[ ! -f "$JOB" ]]; then
    echo "No existe el job: $JOB"
    exit 1
fi

JOB_REL="${JOB#${PROJECT_DIR}/}"

docker run --rm -it \
    --network host \
    -v "${PROJECT_DIR}:/workspace/delta-to-sql" \
    -w /workspace/delta-to-sql \
    -e KUBECONFIG=/workspace/delta-to-sql/.kube/config \
    -v "$HOME/.m2:/root/.m2" \
    -e SPARK_EXTRA_CLASSPATH="/workspace/delta-to-sql/lib/bcprov-jdk18on-1.80.jar:/workspace/delta-to-sql/lib/bcpkix-jdk18on-1.80.jar:/workspace/delta-to-sql/lib/bcutil-jdk18on-1.80.jar" \
    mi-data-working:3.5.7 \
    bash -lc '
      set -euo pipefail

      spark-submit \
        --master k8s://https://host.docker.internal:35909 \
        --deploy-mode cluster \
        --name delta-to-sql \
        --conf spark.kubernetes.namespace=ed-isdf \
        --conf spark.kubernetes.container.image=mi-data-runtime:3.5.7 \
        --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent \
        --conf spark.kubernetes.authenticate.driver.serviceAccountName=working \
        --conf spark.kubernetes.driver.secretKeyRef.AWS_ACCESS_KEY_ID=lakehouse-s3:AWS_ACCESS_KEY_ID \
        --conf spark.kubernetes.driver.secretKeyRef.AWS_SECRET_ACCESS_KEY=lakehouse-s3:AWS_SECRET_ACCESS_KEY \
        --conf spark.kubernetes.driver.secretKeyRef.POSTGRES_HOST=postgres-isdf:POSTGRES_HOST \
        --conf spark.kubernetes.driver.secretKeyRef.POSTGRES_PORT=postgres-isdf:POSTGRES_PORT \
        --conf spark.kubernetes.driver.secretKeyRef.POSTGRES_DATABASE=postgres-isdf:POSTGRES_DATABASE \
        --conf spark.kubernetes.driver.secretKeyRef.POSTGRES_USER=postgres-isdf:POSTGRES_USER \
        --conf spark.kubernetes.driver.secretKeyRef.POSTGRES_PASSWORD=postgres-isdf:POSTGRES_PASSWORD \
        --conf spark.kubernetes.driver.secretKeyRef.POSTGRES_SSLMODE=postgres-isdf:POSTGRES_SSLMODE \
        --conf spark.hadoop.fs.s3a.endpoint=http://seaweedfs-s3.seaweedfs.svc.cluster.local:8333 \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
        --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.EnvironmentVariableCredentialsProvider \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        --py-files /workspace/delta-to-sql/build/delta-to-sql.zip \
        /workspace/delta-to-sql/'"${JOB_REL}"' \
        --config /workspace/delta-to-sql/config/config.yaml \
        "$@"
    ' -- "$@"
