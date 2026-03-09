ARG target_base_image
ARG crs_version

FROM patch-ensemble-base

COPY --from=libcrs . /libCRS
RUN pip3 install /libCRS \
    && python3 -c "from libCRS.base import DataType; print('libCRS OK')"

COPY pyproject.toml /opt/crs-patch-ensemble/pyproject.toml
COPY patcher.py /opt/crs-patch-ensemble/patcher.py
RUN pip3 install /opt/crs-patch-ensemble

CMD ["run_patcher"]
