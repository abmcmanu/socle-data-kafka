package io.socle.ingestion.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.socle.ingestion.service.OrderPublisher;
import io.socle.ingestion.service.PublicationFailedException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(OrderController.class)
class OrderControllerTest {

    private static final String VALID = """
            {"orderId":"O-1","customerId":"C-0001","channel":"WEB","currency":"EUR",
             "items":[{"sku":"SKU-1","quantity":2,"unitPrice":19.90}]}
            """;

    @Autowired
    private MockMvc mvc;

    @MockitoBean
    private OrderPublisher publisher;

    @Test
    void commandeValideRenvoie202() throws Exception {
        when(publisher.publish(any())).thenReturn(
                new OrderAcceptedResponse("O-1", "e-1", "orders.evt.order-placed.v1", 3, 42L));

        mvc.perform(post("/api/v1/orders").contentType(MediaType.APPLICATION_JSON).content(VALID))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.partition").value(3))
                .andExpect(jsonPath("$.offset").value(42));
    }

    @Test
    void commandeSansArticleRenvoie400EtNePublieRien() throws Exception {
        String invalid = """
                {"customerId":"C-0001","channel":"WEB","currency":"EUR","items":[]}
                """;
        mvc.perform(post("/api/v1/orders").contentType(MediaType.APPLICATION_JSON).content(invalid))
                .andExpect(status().isBadRequest());
        verify(publisher, never()).publish(any());
    }

    @Test
    void prixAvecTroisDecimalesRefuse() throws Exception {
        String invalid = VALID.replace("19.90", "19.905");
        mvc.perform(post("/api/v1/orders").contentType(MediaType.APPLICATION_JSON).content(invalid))
                .andExpect(status().isBadRequest());
    }

    @Test
    void kafkaIndisponibleRenvoie503() throws Exception {
        when(publisher.publish(any())).thenThrow(
                new PublicationFailedException("timeout", "e-1", true, new RuntimeException()));

        mvc.perform(post("/api/v1/orders").contentType(MediaType.APPLICATION_JSON).content(VALID))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.retriable").value(true));
    }
}
