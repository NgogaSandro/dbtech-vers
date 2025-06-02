package de.htwberlin.dbtech.aufgaben.ue03;

import java.math.BigDecimal;
import java.sql.Connection;
import java.time.LocalDate;

public interface IVersicherungsServiceDao {
    void setConnection(Connection connection);
    Integer getProduktIdFromVertrag(Integer vertragsId);
    Integer getProduktIdFromDeckungsart(Integer deckungsartId);
    void checkDeckungsbetragExistiert(Integer deckungsartId, BigDecimal deckungsbetrag);
    LocalDate getVersicherungsbeginn(Integer vertragsId);
    void checkDeckungspreisExistiert(Integer deckungsartId, BigDecimal deckungsbetrag, LocalDate versicherungsbeginn);
    Integer getKundenIdFromVertrag(Integer vertragsId);
    LocalDate getGeburtsdatumFromKunde(Integer kundenId);
    void insertDeckung(Integer vertragsId, Integer deckungsartId, BigDecimal deckungsbetrag);
}
