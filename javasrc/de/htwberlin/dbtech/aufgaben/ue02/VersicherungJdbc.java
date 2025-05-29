package de.htwberlin.dbtech.aufgaben.ue02;


/*
  @author Ingo Classen
 */

import de.htwberlin.dbtech.exceptions.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * VersicherungJdbc
 */
public class VersicherungJdbc implements IVersicherungJdbc {
    private static final Logger L = LoggerFactory.getLogger(VersicherungJdbc.class);
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
    public List<String> kurzBezProdukte() {
        L.info("start");
        String sql = "select KurzBez from Produkt order by id ";
        L.info(sql);
        List<String> result = new ArrayList<>();
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    String kurzBez = rs.getString("KurzBez");
                    result.add(kurzBez);
                }
            }
        } catch (SQLException e) {
            L.error("", e);
            throw new DataException(e);
        }
        L.info("ende");
        return result;
    }

    @Override
    public Kunde findKundeById(Integer id) {
        L.info("id: " + id);
        String sql = "select * from Kunde where id= " + id;
        L.info(sql);
        Kunde kunde = null;
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    String name = rs.getString("Name");
                    LocalDate geburtsdatum = rs.getDate("Geburtsdatum").toLocalDate();
                    kunde = new Kunde(id, name, geburtsdatum);
                } else {
                    throw new KundeExistiertNichtException(id);
                }
            }
        } catch (SQLException e) {
            L.error("", e);
            throw new DataException(e);
        }
        L.info("ende");
        return kunde;
    }

    @Override
    public void createVertrag(Integer id, Integer produktId, Integer kundenId, LocalDate versicherungsbeginn) {
        L.info("id: " + id);
        L.info("produktId: " + produktId);
        L.info("kundenId: " + kundenId);
        L.info("versicherungsbeginn: " + versicherungsbeginn);
        L.info("Start insert");

        String sql = "insert into Vertrag(id, produkt_fk, kunde_fk, versicherungsbeginn, versicherungsende) values (?, ?, ?, ?, ?)";
        L.info(sql);

        if (versicherungsbeginn.isBefore(LocalDate.now())) {
            throw new DatumInVergangenheitException(versicherungsbeginn);
        }

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {

            pstmt.setInt(1, id);
            pstmt.setInt(2, produktId);
            pstmt.setInt(3, kundenId);
            pstmt.setDate(4, java.sql.Date.valueOf(versicherungsbeginn));
            pstmt.setDate(5, java.sql.Date.valueOf(versicherungsbeginn.plusYears(1).minusDays(1)));
            pstmt.executeUpdate();
        } catch (SQLIntegrityConstraintViolationException e) {
            if (e.getMessage().contains("KUNDE_FK")) {
                throw new KundeExistiertNichtException(kundenId);
            } else if (e.getMessage().contains("PRODUKT_FK")) {
                throw new ProduktExistiertNichtException(produktId);
            } else if (e.getMessage().contains("ID")) {
                throw new VertragExistiertBereitsException(id);
            } else {
                L.error("", e);
                throw new DataException(e);
            }
        } catch (SQLException e) {
            L.error("", e);
            throw new DataException(e);
        }
        L.info("ende");
    }

    @Override
    public BigDecimal calcMonatsrate(Integer vertragsId) {
        L.info("vertragsId: " + vertragsId);
        String sql =
                "select sum(dp.preis) as preis "+
                        "from Deckungspreis dp " +
                        "join Deckungsbetrag db on db.id = dp.deckungsbetrag_fk "  +
                        "join Deckungsart da on da.id = db.deckungsart_fk " +
                        "join Deckung d on d.deckungsart_fk = da.id " +
                        "join Vertrag v on d.vertrag_fk = v.id " +
                        "where v.id = " +  vertragsId + " and " +
                        "EXTRACT(YEAR From v.versicherungsbeginn) >= EXTRACT(YEAR From dp.gueltig_von) and EXTRACT(YEAR From v.versicherungsbeginn) <= EXTRACT(YEAR From dp.gueltig_bis) " +
                        "group by v.id, v.versicherungsbeginn";

        try (Statement stmt = connection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    BigDecimal deckungspreis = rs.getBigDecimal("Preis");
                    L.info("Deckungspreis: " + deckungspreis);
                    return deckungspreis;
                }
            }
        } catch (SQLException e) {
            L.error("", e);
            throw new DataException(e);
        }
        L.info("ende");
        return BigDecimal.ZERO;
    }
}