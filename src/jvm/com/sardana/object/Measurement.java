package com.sardana.object;

/**
 * @author Sardana
 */
public class Measurement {
    private Long version;
    private String layer;
    private Long processingTimestamp;
    private Long measurementTimestamp;
    private String application;
    private Value value;

    public static class Value {
        private Long seqNr;
        private Integer typeNr;
        private String sourceMac;
        private String droneId;
        private Integer localMac;
        private Integer signal;
        private Integer subTypeNr;
        private boolean retryFlag;

        public Long getSeqNr() {
            return seqNr;
        }

        public void setSeqNr(Long seqNr) {
            this.seqNr = seqNr;
        }

        public Integer getTypeNr() {
            return typeNr;
        }

        public void setTypeNr(Integer typeNr) {
            this.typeNr = typeNr;
        }

        public String getSourceMac() {
            return sourceMac;
        }

        public void setSourceMac(String sourceMac) {
            this.sourceMac = sourceMac;
        }

        public String getDroneId() {
            return droneId;
        }

        public void setDroneId(String droneId) {
            this.droneId = droneId;
        }

        public Integer getLocalMac() {
            return localMac;
        }

        public void setLocalMac(Integer localMac) {
            this.localMac = localMac;
        }

        public Integer getSignal() {
            return signal;
        }

        public void setSignal(Integer signal) {
            this.signal = signal;
        }

        public Integer getSubTypeNr() {
            return subTypeNr;
        }

        public void setSubTypeNr(Integer subTypeNr) {
            this.subTypeNr = subTypeNr;
        }

        public boolean isRetryFlag() {
            return retryFlag;
        }

        public void setRetryFlag(boolean retryFlag) {
            this.retryFlag = retryFlag;
        }
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public String getLayer() {
        return layer;
    }

    public void setLayer(String layer) {
        this.layer = layer;
    }

    public Long getProcessingTimestamp() {
        return processingTimestamp;
    }

    public void setProcessingTimestamp(Long processingTimestamp) {
        this.processingTimestamp = processingTimestamp;
    }

    public Long getMeasurementTimestamp() {
        return measurementTimestamp;
    }

    public void setMeasurementTimestamp(Long measurementTimestamp) {
        this.measurementTimestamp = measurementTimestamp;
    }

    public String getApplication() {
        return application;
    }

    public void setApplication(String application) {
        this.application = application;
    }

    public Value getValue() {
        return value;
    }

    public void setValue(Value value) {
        this.value = value;
    }
}
