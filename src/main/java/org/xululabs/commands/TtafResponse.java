package org.xululabs.commands;

public class TtafResponse {
    private Integer remApiLimits;
    private String commandDetails;
    private Object responseData;

    /**
     * Constructs TtafResponse Instance with default property values e.g. null
     */
    public TtafResponse() {
    }

    /**
     * Constructs TtafResponse instance with Object ResponseData
     * 
     * @param responseData
     */
    public TtafResponse(Object responseData) {
        this(null, responseData);
    }

    /**
     * Constructs TtafResponse Instance with Integer remApiLimits and Object
     * ResponseData
     * 
     * @param remApiLimits
     * @param responseData
     */
    public TtafResponse(Integer remApiLimits, Object responseData) {
        this.remApiLimits = remApiLimits;
        this.responseData = responseData;
    }

    /**
     * Returns Object ResponseData
     * 
     * @return Object ResponseData
     */
    public Object getResponseData() {
        return responseData;
    }

    /**
     * Sets ResponseData
     * 
     * @param responseData
     */
    public void setResponseData(Object responseData) {
        this.responseData = responseData;
    }

}
