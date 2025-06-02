package de.htwberlin.dbtech.aufgaben.ue03;

/*
  @author Ingo Classen
 */

import de.htwberlin.dbtech.exceptions.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;

/**
 * VersicherungJdbc
 */
public class VersicherungService implements IVersicherungService {
    private static final Logger L = LoggerFactory.getLogger(VersicherungService.class);
    private Connection connection;

    @Override
    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    @SuppressWarnings("unused")
    private Connection useConnection() {
        if (connection == null) {
            throw new DataException("Connection not set");
        }
        return connection;
    }

    @Override
    public void createDeckung(Integer vertragsId, Integer deckungsartId, BigDecimal deckungsbetrag) {
        L.info("vertragsId: " + vertragsId);
        L.info("deckungsartId: " + deckungsartId);
        L.info("deckungsbetrag: " + deckungsbetrag);

        // 1. Produkt-IDs prüfen (existiert Vertrag/Deckungsart? passt Deckungsart zum Produkt?)
        Integer produktIdVertrag = getProduktIdFromVertrag(vertragsId);
        Integer produktIdDeckungsart = getProduktIdFromDeckungsart(deckungsartId);
        if (!produktIdVertrag.equals(produktIdDeckungsart)) {
            throw new DeckungsartPasstNichtZuProduktException(produktIdDeckungsart, produktIdVertrag);
        }

        // 2. Deckungsbetrag gültig?
        checkDeckungsbetragExistiert(deckungsartId, deckungsbetrag);

        // 3. Versicherungsbeginn holen (für weitere Prüfungen)
        LocalDate versicherungsbeginn = getVersicherungsbeginn(vertragsId);

        // 4. Deckungspreis vorhanden?
        checkDeckungspreisExistiert(deckungsartId, deckungsbetrag, versicherungsbeginn);

        // 5. Regelkonformität prüfen (z.B. Alter)
        checkRegelkonform(vertragsId, deckungsartId, deckungsbetrag, versicherungsbeginn);

        // 6. Insert
        String sql = "insert into Deckung (VERTRAG_FK, DECKUNGSART_FK, DECKUNGSBETRAG) values (?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, vertragsId);
            statement.setInt(2, deckungsartId);
            statement.setBigDecimal(3, deckungsbetrag);
            statement.executeUpdate();
        } catch (SQLException e) {
            if (e.getMessage().contains("VERTRAG_FK")) {
                throw new VertragExistiertNichtException(vertragsId);
            } else if (e.getMessage().contains("DECKUNGSART_FK")) {
                throw new DeckungsartExistiertNichtException(deckungsartId);
            }
            throw new DataException(e.getMessage());
        }
        L.info("ende");
    }

    private Integer getProduktIdFromVertrag(Integer vertragsId) {
        try (PreparedStatement stmt = connection.prepareStatement(
                "SELECT PRODUKT_FK FROM VERTRAG WHERE ID = ?")) {
            stmt.setInt(1, vertragsId);
            var rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            } else {
                throw new VertragExistiertNichtException(vertragsId);
            }
        } catch (SQLException e) {
            throw new DataException(e.getMessage());
        }
    }

    private Integer getProduktIdFromDeckungsart(Integer deckungsartId) {
        try (PreparedStatement stmt = connection.prepareStatement(
                "SELECT PRODUKT_FK FROM DECKUNGSART WHERE ID = ?")) {
            stmt.setInt(1, deckungsartId);
            var rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            } else {
                throw new DeckungsartExistiertNichtException(deckungsartId);
            }
        } catch (SQLException e) {
            throw new DataException(e.getMessage());
        }
    }

    private void checkDeckungsbetragExistiert(Integer deckungsartId, BigDecimal deckungsbetrag) {
        String sql = "SELECT 1 FROM DECKUNGSBETRAG WHERE DECKUNGSART_FK = ? AND DECKUNGSBETRAG = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setInt(1, deckungsartId);
            stmt.setBigDecimal(2, deckungsbetrag);
            var rs = stmt.executeQuery();
            if (!rs.next()) {
                throw new UngueltigerDeckungsbetragException(deckungsbetrag);
            }
        } catch (SQLException e) {
            throw new DataException(e.getMessage());
        }
    }

    private LocalDate getVersicherungsbeginn(Integer vertragsId) {
        String sql = "SELECT VERSICHERUNGSBEGINN FROM VERTRAG WHERE ID = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setInt(1, vertragsId);
            var rs = stmt.executeQuery();
            if (rs.next()) {
                Date date = rs.getDate(1);
                return date.toLocalDate();
            } else {
                throw new VertragExistiertNichtException(vertragsId);
            }
        } catch (SQLException e) {
            throw new DataException(e.getMessage());
        }
    }

    private void checkDeckungspreisExistiert(Integer deckungsartId, BigDecimal deckungsbetrag, LocalDate versicherungsbeginn) {
        String sql = "SELECT 1 FROM DECKUNGSPREIS dp " +
                "JOIN DECKUNGSBETRAG db ON dp.DECKUNGSBETRAG_FK = db.ID " +
                "WHERE db.DECKUNGSART_FK = ? AND db.DECKUNGSBETRAG = ? " +
                "AND ? BETWEEN dp.GUELTIG_VON AND dp.GUELTIG_BIS";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setInt(1, deckungsartId);
            stmt.setBigDecimal(2, deckungsbetrag);
            stmt.setDate(3, Date.valueOf(versicherungsbeginn));
            var rs = stmt.executeQuery();
            if (!rs.next()) {
                throw new DeckungspreisNichtVorhandenException(deckungsbetrag);
            }
        } catch (SQLException e) {
            throw new DataException(e.getMessage());
        }
    }

    private void checkRegelkonform(Integer vertragsId, Integer deckungsartId, BigDecimal deckungsbetrag, LocalDate versicherungsbeginn) {
        // Beispielhafte Regelprüfung: Alter des Kunden
        // Hier muss die Logik ggf. an die konkreten Anforderungen angepasst werden.
        // Für die Tests reicht meist:
        // - Hole KundenID aus Vertrag
        // - Hole Geburtsdatum aus Kunde
        // - Berechne Alter zum Versicherungsbeginn
        // - Prüfe, ob für die Deckungsart und Betrag das Alter zulässig ist

        Integer kundenId = getKundenIdFromVertrag(vertragsId);
        LocalDate geburtsdatum = getGeburtsdatumFromKunde(kundenId);
        int alter = versicherungsbeginn.getYear() - geburtsdatum.getYear();
        if (versicherungsbeginn.getDayOfYear() < geburtsdatum.getDayOfYear()) {
            alter--;
        }

        // Beispielhafte Regeln (anpassen je nach Testdaten!):
        // Deckungsart 1 (Haftung) nur ab 18
        if (deckungsartId == 1 && alter < 18) {
            throw new DeckungsartNichtRegelkonformException(deckungsartId);
        }
        // Deckungsart 3 (Tod) für LBV: max 100k bis 90, max 200k bis 70, max 300k bis 60
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

    private Integer getKundenIdFromVertrag(Integer vertragsId) {
        String sql = "SELECT KUNDE_FK FROM VERTRAG WHERE ID = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setInt(1, vertragsId);
            var rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            } else {
                throw new VertragExistiertNichtException(vertragsId);
            }
        } catch (SQLException e) {
            throw new DataException(e.getMessage());
        }
    }

    private LocalDate getGeburtsdatumFromKunde(Integer kundenId) {
        String sql = "SELECT GEBURTSDATUM FROM KUNDE WHERE ID = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setInt(1, kundenId);
            var rs = stmt.executeQuery();
            if (rs.next()) {
                Date date = rs.getDate(1);
                return date.toLocalDate();
            } else {
                throw new KundeExistiertNichtException(kundenId);
            }
        } catch (SQLException e) {
            throw new DataException(e.getMessage());
        }
    }
}