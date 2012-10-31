package secondlc;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.EventType;
import org.hibernate.event.spi.PreLoadEvent;
import org.hibernate.event.spi.PreLoadEventListener;
import org.hibernate.integrator.spi.Integrator;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.mapping.Collection;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.metamodel.source.MetadataImplementor;
import org.hibernate.service.BootstrapServiceRegistryBuilder;
import org.hibernate.service.internal.StandardServiceRegistryImpl;
import org.hibernate.service.spi.SessionFactoryServiceRegistry;
import org.hibernate.stat.SecondLevelCacheStatistics;
import org.hibernate.test.cache.infinispan.functional.Age;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;

import javax.transaction.TransactionManager;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.infinispan.test.TestingUtil.withTx;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
public abstract class BaseStalePutFromLoadCacheTest {

   static final Log log = LogFactory.getLog(BaseStalePutFromLoadCacheTest.class);

   SessionFactory sf;
   TransactionManager tm;

   final CountDownLatch deleteWait = new CountDownLatch(1);
   final CountDownLatch postLoadContinue = new CountDownLatch(1);

   @Before
   public void beforeClass() {
      Configuration cfg = new Configuration();
      cfg.setProperty(Environment.USE_SECOND_LEVEL_CACHE, "true");
      cfg.setProperty(Environment.USE_QUERY_CACHE, "true");

      configureCache(cfg);

      // Configure mappings
      configureMappings(cfg);

      // Create database schema in each run
      cfg.setProperty(Environment.HBM2DDL_AUTO, "create-drop");

      // Configure registry
      sf =  cfg.buildSessionFactory(configureRegistry(cfg));

      tm = com.arjuna.ats.jta.TransactionManager.transactionManager();
   }

   abstract void configureCache(Configuration cfg);

   private void configureMappings(Configuration cfg) {
      String[] mappings = getMappings();
      if (mappings != null) {
         for (String mapping : mappings) {
            cfg.addResource(getBaseForMappings() + mapping,
                  getClass().getClassLoader());
         }
      }

      Class<?>[] annotatedClasses = getAnnotatedClasses();
      if (annotatedClasses != null) {
         for (Class<?> annotatedClass : annotatedClasses) {
            cfg.addAnnotatedClass(annotatedClass);
         }
      }

      cfg.buildMappings();
      Iterator it = cfg.getClassMappings();
      while (it.hasNext()) {
         PersistentClass clazz = (PersistentClass) it.next();
         if (!clazz.isInherited()) {
            cfg.setCacheConcurrencyStrategy(clazz.getEntityName(),
                  getCacheConcurrencyStrategy());
         }
      }
      it = cfg.getCollectionMappings();
      while (it.hasNext()) {
         Collection coll = (Collection) it.next();
         cfg.setCollectionCacheConcurrencyStrategy(coll.getRole(),
               getCacheConcurrencyStrategy());
      }
   }

   public Class[] getAnnotatedClasses() {
      return new Class[] { Age.class };
   }

   public String[] getMappings() {
      return null;
   }

   protected String getBaseForMappings() {
      return "org/hibernate/test/";
   }

   public String getCacheConcurrencyStrategy() {
      return "read-only";
   }

   private StandardServiceRegistryImpl configureRegistry(Configuration cfg) {
      BootstrapServiceRegistryBuilder builder =
            new BootstrapServiceRegistryBuilder();
      prepareBootstrapRegistryBuilder(builder);

      Properties properties = new Properties();
      properties.putAll(cfg.getProperties());
      Environment.verifyProperties(properties);
      ConfigurationHelper.resolvePlaceHolders(properties);

      org.hibernate.service.ServiceRegistryBuilder registryBuilder =
            new org.hibernate.service.ServiceRegistryBuilder(builder.build())
                  .applySettings(properties);

      return (StandardServiceRegistryImpl) registryBuilder.buildServiceRegistry();
   }

   private void prepareBootstrapRegistryBuilder(
         BootstrapServiceRegistryBuilder builder) {
      builder.with(new Integrator() {
         @Override
         public void integrate(Configuration configuration,
               SessionFactoryImplementor sessionFactory,
               SessionFactoryServiceRegistry serviceRegistry) {
            integrate(serviceRegistry);
         }

         @Override
         public void integrate(MetadataImplementor metadata,
               SessionFactoryImplementor sessionFactory,
               SessionFactoryServiceRegistry serviceRegistry) {
            integrate(serviceRegistry);
         }

         @Override
         public void disintegrate(SessionFactoryImplementor sessionFactory,
               SessionFactoryServiceRegistry serviceRegistry) {
            // Empty
         }

         private void integrate(SessionFactoryServiceRegistry serviceRegistry) {
            serviceRegistry.getService(EventListenerRegistry.class)
                  .appendListeners(EventType.PRE_LOAD, new SlowPreLoad());
         }
      });
   }

   @Test
   public void testNoStaleDataFromCache() throws Exception {
      // 1. Store an entity instance
      final Age age = new Age();
      age.setAge(98);

      withTx(tm, new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            Session s = sf.openSession();
            s.getTransaction().begin();
            s.persist(age);
            s.getTransaction().commit();
            s.close();
            return null;
         }
      });

      // 2. Clear the 2LC region so that needs load comes from DB
      sf.getCache().evictEntityRegion(Age.class);

      // 3. Clear statistics
      sf.getStatistics().clear();
      final SecondLevelCacheStatistics stats = sf.getStatistics()
            .getSecondLevelCacheStatistics(Age.class.getName());

      ExecutorService exec = Executors.newFixedThreadPool(2);
      Future<Void> loadFromDbFuture = exec.submit(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            // 4. Load entity making sure it comes from database
            log.debug("Load entity");
            withTx(tm, new Callable<Object>() {
               @Override
               public Object call() throws Exception {
                  Session s = sf.openSession();
                  s.getTransaction().begin();
                  Age found = (Age) s.load(Age.class, age.getId());
                  assertEquals(age.getAge(), found.getAge());
                  assertEquals(1, stats.getMissCount());
                  // A miss happens but whether the put happens or not depends
                  // on whether the 2LC implementation allows stale data to
                  // be cached. So, commenting for the moment and a later
                  // check will verify it.
                  //
                  // assertEquals(0, stats.getPutCount());
                  assertEquals(0, stats.getHitCount());
                  s.getTransaction().commit();
                  s.close();
                  return null;
               }
            });
            return null;
         }
      });

      Future<Void> deleteFuture = exec.submit(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            log.debug("Wait for delete to be allowed to go through");
            // Wait for removal to be allowed
            deleteWait.await(60, TimeUnit.SECONDS);

            log.debug("Delete wait finished, delete all instances via HQL");
            // 5. Remove all entities using HQL
            withTx(tm, new Callable<Void>() {
               @Override
               public Void call() throws Exception {
                  Session s = sf.openSession();
                  s.getTransaction().begin();
                  // Remove via HQL
                  int i = s.createQuery("delete from Age").executeUpdate();
                  assertEquals(1, i);
                  s.getTransaction().commit();
                  s.close();
                  return null;
               }
            });

            log.debug("Let the putFromLoad for entity continue");
            // Let the post load continue
            postLoadContinue.countDown();

            return null;
         }
      });

      deleteFuture.get(90, TimeUnit.SECONDS);
      loadFromDbFuture.get(90, TimeUnit.SECONDS);

      // 6. Verify that no entities are found now
      Session s = sf.openSession();
      assertNull(s.get(Age.class, age.getId()));
      s.close();
   }

   public class SlowPreLoad implements PreLoadEventListener {

      @Override
      public void onPreLoad(PreLoadEvent event) {
         try {
            // Let the remove thread go through
            deleteWait.countDown();
            log.debug("Delete can now go through");
            log.debug("Wait for putFromLoad to be allowed to continue");
            postLoadContinue.await(60, TimeUnit.SECONDS);
            log.debug("putFromLoad can now continue");
         } catch (InterruptedException e) {
            // Restore interrupted status
            Thread.currentThread().interrupt();
         }
      }

   }

}
