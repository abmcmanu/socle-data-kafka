package io.socle.enrichment;

/** Noms des topics : doivent correspondre à governance/topics/catalog.yaml. */
public final class Topics {
    public static final String ORDERS_PLACED = "orders.evt.order-placed.v1";
    public static final String ORDERS_DLQ = "orders.dlq.order-placed.v1";
    public static final String CUSTOMERS = "crm.cdc.customers.v1";
    public static final String ORDERS_ENRICHED = "orders.evt.order-enriched.v1";
    public static final String REVENUE_BY_COUNTRY = "analytics.agg.revenue-by-country-1m.v1";

    private Topics() {
    }
}
