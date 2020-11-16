FROM makerdao/vdb-builder as builder

WORKDIR /vulcanizedb
COPY . .
RUN go build

FROM makerdao/vdb-runner

# Direct logs to stdout for docker log driver
RUN ln -sf /dev/stdout /app/vulcanizedb.log

# add required files
COPY --from=builder /vulcanizedb/vulcanizedb .
COPY --from=builder /vulcanizedb/dockerfiles/reset_header_check_count/startup_script.sh .

# need to execute with a shell to access env variables
CMD ["./startup_script.sh"]
