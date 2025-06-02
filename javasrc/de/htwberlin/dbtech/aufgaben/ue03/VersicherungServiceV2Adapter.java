// Datei: javasrc/de/htwberlin/dbtech/aufgaben/ue03/VersicherungServiceV2Adapter.java
package de.htwberlin.dbtech.aufgaben.ue03;

import de.htwberlin.dbtech.exceptions.*;
import java.math.BigDecimal;
import java.sql.Connection;

public class VersicherungServiceV2Adapter implements IVersicherungService {
    private final IVersicherungsServiceDao dao = new VersicherungServiceV2();

    @Override
    public void setConnection(Connection connection) {
        dao.setConnection(connection);
    }

    @Override
    public void createDeckung(Integer vertragsId, Integer deckungsartId, BigDecimal deckungsbetrag) {
        // Die Logik aus VersicherungService übernehmen:
        Integer produktIdVertrag = dao.getProduktIdFromVertrag(vertragsId);
        Integer produktIdDeckungsart = dao.getProduktIdFromDeckungsart(deckungsartId);
        if (!produktIdVertrag.equals(produktIdDeckungsart)) {
            throw new DeckungsartPasstNichtZuProduktException(produktIdDeckungsart, produktIdVertrag);
        }

        dao.checkDeckungsbetragExistiert(deckungsartId, deckungsbetrag);

        var versicherungsbeginn = dao.getVersicherungsbeginn(vertragsId);

        dao.checkDeckungspreisExistiert(deckungsartId, deckungsbetrag, versicherungsbeginn);

        checkRegelkonform(vertragsId, deckungsartId, deckungsbetrag, versicherungsbeginn);

        dao.insertDeckung(vertragsId, deckungsartId, deckungsbetrag);
    }

    private void checkRegelkonform(Integer vertragsId, Integer deckungsartId, BigDecimal deckungsbetrag, java.time.LocalDate versicherungsbeginn) {
        Integer kundenId = dao.getKundenIdFromVertrag(vertragsId);
        java.time.LocalDate geburtsdatum = dao.getGeburtsdatumFromKunde(kundenId);
        int alter = versicherungsbeginn.getYear() - geburtsdatum.getYear();
        if (versicherungsbeginn.getDayOfYear() < geburtsdatum.getDayOfYear()) {
            alter--;
        }
        if (deckungsartId == 1 && alter < 18) {
            throw new DeckungsartNichtRegelkonformException(deckungsartId);
        }
        if (deckungsartId == 3) {
            if (deckungsbetrag.intValue() == 100_000 && alter > 90) {
                throw new DeckungsartNichtRegelkonformException(deckungsartId);
            }
            if (deckungsbetrag.intValue() == 200_000 && alter > 70) {
                throw new DeckungsartNichtRegelkonformException(deckungsartId);
            }
            if (deckungsbetrag.intValue() == 300_000 && alter > 60) {
                throw new DeckungsartNichtRegelkonformException(deckungsartId);
            }
        }
    }
}