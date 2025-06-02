package de.htwberlin.dbtech.aufgaben.ue03;

import de.htwberlin.dbtech.exceptions.*;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;

public class VersicherungServiceV2 implements IVersicherungsServiceDao{
    private Connection connection;

    @Override
    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    @Override
    public Integer getProduktIdFromVertrag(Integer vertragsId) {
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

    @Override
    public Integer getProduktIdFromDeckungsart(Integer deckungsartId) {
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

    @Override
    public void checkDeckungsbetragExistiert(Integer deckungsartId, BigDecimal deckungsbetrag) {
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

    @Override
    public LocalDate getVersicherungsbeginn(Integer vertragsId) {
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

    @Override
    public void checkDeckungspreisExistiert(Integer deckungsartId, BigDecimal deckungsbetrag, LocalDate versicherungsbeginn) {
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

    @Override
    public Integer getKundenIdFromVertrag(Integer vertragsId) {
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

    @Override
    public LocalDate getGeburtsdatumFromKunde(Integer kundenId) {
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

    @Override
    public void insertDeckung(Integer vertragsId, Integer deckungsartId, BigDecimal deckungsbetrag) {
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
    }
}
