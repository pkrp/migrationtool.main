package main;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.ejb.Stateless;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import model.DatafileEmbedded;
import model.DatafileParameternosql;
import model.Datafilenosql;
import model.Dataset;
import model.DatasetEmbedded;
import model.Datasetnosql;
import model.Facilitynosql;
import model.Datafile;
import model.Facility;

import org.apache.log4j.Logger;

import model.DatafileParameter;

@Stateless
public class MigrationTool {

	private static final Logger log = Logger.getLogger(MigrationTool.class);

	private EntityManager entityManager;

	public EntityManager createEntityManager() {
		EntityManagerFactory factory = Persistence.createEntityManagerFactory("icat");
		return factory.createEntityManager();
	}

	public void testCreate() {
		EntityManagerFactory factory1 = Persistence.createEntityManagerFactory("icat");
		this.entityManager = factory1.createEntityManager();
		entityManager.clear();
		List<DatafileParameternosql> datafiles = new ArrayList<DatafileParameternosql>();

		List<DatafileParameternosql> datafiless = new ArrayList<DatafileParameternosql>();
		for (int i = 0; i <= 500; i++) {

			DatafileParameternosql datafile = new DatafileParameternosql();
			datafile.setCreate_Id("123");
			datafile.setCreate_Time(new Date());
			datafile.setId(new Long(1234 + i * 123234234 + 23122));
			datafile.setMod_Time(new Date());
			datafile.setDatafile_id(new Long(134234));
			datafile.setDatetime_value(new Date());
			datafile.setError(2.2);
			datafile.setNumeric_value(123.2);
			datafile.setString_value("");
			datafiless.add(datafile);
		}
		Instant start = Instant.now();
		entityManager.getTransaction().begin();
		for (DatafileParameternosql d : datafiless) {

			entityManager.persist(d);

		}
		entityManager.getTransaction().commit();
		Instant end = Instant.now();
		double duration = Duration.between(start, end).toMillis() / 1000.0;
		System.out.println(duration);
		System.out.println(datafiles.size());
	}

	public void migrateDatafiles() {
		EntityManagerFactory factory1 = Persistence.createEntityManagerFactory("icat");
		this.entityManager = factory1.createEntityManager();
		log.info(entityManager);
		entityManager.clear();
		Query query = entityManager.createQuery("Select e FROM Datafile e");
		int pageNumber = 1;
		int pageSize = 100000;
		try {
			while (true) {

				query.setFirstResult((pageNumber - 1) * pageSize);
				query.setMaxResults(pageSize);
				List<Datafile> result = query.getResultList();
				if (result.size() == 0)
					break;
				entityManager.getTransaction().begin();
				List<Datafilenosql> datafiles = new ArrayList<Datafilenosql>();
				for (Datafile f : result) {

					Datafilenosql datafile = new Datafilenosql();
					datafile.setCreate_Id(f.getCreateId());
					datafile.setCreate_Time(f.getCreateTime());
					datafile.setChecksum(f.getChecksum());
					datafile.setDatafilecreatetime(f.getDatafileCreateTime());
					datafile.setDatafilemodtime(f.getDatafileModTime());
					datafile.setDescription(f.getDescription());
					datafile.setDoi(f.getDoi());
					datafile.setFilesize(f.getFileSize());
					datafile.setLocation(f.getLocation());
					if (f.getDatafileFormat() != null)
						datafile.setDatafileformat_id(f.getDatafileFormat().getId());
					// datafile.setDataset_id(f.getDataset().getId());
					datafile.setId(f.getId());
					datafile.setMod_Id(f.getModId());
					datafile.setMod_Time(f.getModTime());
					datafile.setName(f.getName());
					entityManager.persist(datafile);

				}
				entityManager.flush();
				entityManager.clear();
				entityManager.getTransaction().commit();
				pageNumber++;
				log.info(pageNumber);
			}
		} finally {
			entityManager.close();
		}
	}

	public void migrateDatafileParameters() {
		EntityManagerFactory factory1 = Persistence.createEntityManagerFactory("icat");
		this.entityManager = factory1.createEntityManager();
		log.info(entityManager);
		entityManager.clear();
		Query query = entityManager.createQuery("Select e FROM DatafileParameter e");
		int pageNumber = 1;
		int pageSize = 100000;
		try {
			while (true) {

				query.setFirstResult((pageNumber - 1) * pageSize);
				query.setMaxResults(pageSize);
				List<DatafileParameter> result = query.getResultList();
				if (result.size() == 0)
					break;
				entityManager.getTransaction().begin();
				List<DatafileParameternosql> datafiles = new ArrayList<DatafileParameternosql>();
				for (DatafileParameter f : result) {

					DatafileParameternosql datafile = new DatafileParameternosql();
					datafile.setCreate_Id(f.getCreateId());
					datafile.setCreate_Time(f.getCreateTime());
					datafile.setId(f.getId());
					datafile.setMod_Id(f.getModId());
					datafile.setMod_Time(f.getModTime());
					datafile.setDatafile_id(f.getDatafile().getId());
					datafile.setDatetime_value(f.getDateTimeValue());
					datafile.setError(f.getError());
					datafile.setNumeric_value(f.getNumericValue());
					datafile.setParameter_type_id(f.getType().getId());
					datafile.setRangebottom(f.getRangeBottom());
					datafile.setRangetop(f.getRangeTop());
					datafile.setString_value(f.getStringValue());
					entityManager.persist(datafile);

				}
				entityManager.flush();
				entityManager.clear();
				entityManager.getTransaction().commit();
				pageNumber++;
				log.info(pageNumber);
			}
		} finally {
			entityManager.close();
		}
	}

	public void migrateDatasets() {
		EntityManagerFactory factory1 = Persistence.createEntityManagerFactory("icat");
		this.entityManager = factory1.createEntityManager();
		log.info(entityManager);
		entityManager.clear();
		Query query = entityManager.createQuery("Select e FROM Dataset e");
		int pageNumber = 16;
		int pageSize = 10000;
		try {
			while (true) {

				query.setFirstResult((pageNumber - 1) * pageSize);
				query.setMaxResults(pageSize);
				List<Dataset> result = query.getResultList();
				if (result.size() == 0)
					break;
				entityManager.getTransaction().begin();
				List<Datasetnosql> datafiles = new ArrayList<Datasetnosql>();
				for (Dataset f : result) {

					Datasetnosql dataset = new Datasetnosql();
					dataset.setCreate_Id(f.getCreateId());
					dataset.setCreate_Time(f.getCreateTime());
					dataset.setDescription(f.getDescription());
					dataset.setDoi(f.getDoi());
					dataset.setLocation(f.getLocation());
					dataset.setInvestigation_id(f.getInvestigation().getId());
					dataset.setId(f.getId());
					dataset.setMod_Id(f.getModId());
					dataset.setMod_Time(f.getModTime());
					dataset.setName(f.getName());
					dataset.setComplete(f.isComplete());
					dataset.setEndDate(f.getEndDate());
					dataset.setStartDate(f.getStartDate());
					if (f.getSample() != null)
						dataset.setSample_id(f.getSample().getId());
					dataset.setType_id(f.getType().getId());
					entityManager.persist(dataset);

				}
				entityManager.flush();
				entityManager.clear();
				entityManager.getTransaction().commit();
				pageNumber++;
				log.info(pageNumber);
			}
		} finally {
			entityManager.close();
		}
	}

	public void migrateEmbedded() {
		EntityManagerFactory factory1 = Persistence.createEntityManagerFactory("icat");
		this.entityManager = factory1.createEntityManager();
		log.info(entityManager);
		entityManager.clear();
		Query query = entityManager.createQuery("Select e FROM Dataset e where e.id = 24076956");
		int pageNumber = 1;
		int pageSize = 1000;
		try {
			while (true) {

			query.setFirstResult((pageNumber-1) * pageSize);
			query.setMaxResults(pageSize);
			List<Dataset> result = query.getResultList();
			if (result.size() == 0) break;
			entityManager.getTransaction().begin();
			List<DatasetEmbedded> datafiles = new ArrayList<DatasetEmbedded>();
			for (Dataset f : result) {
				Query query1 = entityManager.createQuery("Select e FROM Datafile e WHERE e.dataset.id = :dsid");
				query1.setParameter("dsid", f.getId());
				List<Datafile> result1 = query1.getResultList();
				List<DatafileEmbedded> datafileEmbeddeds = new ArrayList<DatafileEmbedded>();
				for (Datafile ff : result1) {

					DatafileEmbedded datafile = new DatafileEmbedded();
					datafile.setCreate_Id(ff.getCreateId());
					datafile.setCreate_Time(ff.getCreateTime());
					datafile.setChecksum(ff.getChecksum());
					datafile.setDatafilecreatetime(ff.getDatafileCreateTime());
					datafile.setDatafilemodtime(ff.getDatafileModTime());
					datafile.setDescription(ff.getDescription());
					datafile.setDoi(ff.getDoi());
					datafile.setFilesize(ff.getFileSize());
					datafile.setLocation(ff.getLocation());
					if (ff.getDatafileFormat() != null)
						datafile.setDatafileformat_id(ff.getDatafileFormat().getId());
					datafile.setMod_Id(ff.getModId());
					datafile.setMod_Time(ff.getModTime());
					datafile.setName(ff.getName());
					datafileEmbeddeds.add(datafile);
				}
				DatasetEmbedded dataset = new DatasetEmbedded();
				dataset.setCreate_Id(f.getCreateId());
				dataset.setCreate_Time(f.getCreateTime());
				dataset.setDescription(f.getDescription());
				dataset.setDoi(f.getDoi());
				dataset.setLocation(f.getLocation());
				dataset.setInvestigation_id(f.getInvestigation().getId());
				dataset.setId(f.getId());
				dataset.setMod_Id(f.getModId());
				dataset.setMod_Time(f.getModTime());
				dataset.setName(f.getName());
				dataset.setComplete(f.isComplete());
				dataset.setEndDate(f.getEndDate());
				dataset.setStartDate(f.getStartDate());
				dataset.setDatafiles(datafileEmbeddeds);
				if (f.getSample() != null)
					dataset.setSample_id(f.getSample().getId());
				dataset.setType_id(f.getType().getId());
				entityManager.persist(dataset);

				 }
				entityManager.flush();
				entityManager.clear();
				entityManager.getTransaction().commit();
				 pageNumber++;
			}
		} finally {
			entityManager.close();
		}
	}

	public void migrateFacilities() {
		EntityManagerFactory factory = Persistence.createEntityManagerFactory("icat");
		this.entityManager = factory.createEntityManager();
		Query query = entityManager.createQuery("Select e FROM Facility e");
		List<Facility> result = query.getResultList();
		for (Facility f : result) {
			Facilitynosql facility = new Facilitynosql();
			facility.setCreate_id(f.getCreateId());
			facility.setCreate_time(f.getCreateTime());
			facility.setDaysuntilrelease(f.getDaysUntilRelease());
			facility.setDescription(f.getDescription());
			facility.setFullname(f.getFullName());
			facility.setId(String.valueOf(f.getId()));
			facility.setMod_id(f.getModId());
			facility.setMod_time(f.getModTime());
			facility.setName(f.getName());
			facility.setUrl(f.getUrl());
			EntityManagerFactory factory1 = Persistence.createEntityManagerFactory("icat");
			this.entityManager = factory1.createEntityManager();
			entityManager.getTransaction().begin();
			entityManager.persist(facility);
			entityManager.getTransaction().commit();
			entityManager.close();
		}
	}
}
