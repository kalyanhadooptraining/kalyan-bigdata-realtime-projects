-- MySQL dump 10.13  Distrib 5.6.28, for debian-linux-gnu (x86_64)
--
-- Host: localhost    Database: sqoop
-- ------------------------------------------------------
-- Server version	5.6.28-0ubuntu0.15.10.1

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `pet`
--

DROP TABLE IF EXISTS `pet`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `pet` (
  `name` varchar(20) DEFAULT NULL,
  `owner` varchar(20) DEFAULT NULL,
  `species` varchar(20) DEFAULT NULL,
  `sex` char(1) DEFAULT NULL,
  `birth` date DEFAULT NULL,
  `death` date DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `pet`
--

LOCK TABLES `pet` WRITE;
/*!40000 ALTER TABLE `pet` DISABLE KEYS */;
INSERT INTO `pet` VALUES ('Fluffy','Harold','cat','f','1993-02-04','0000-00-00'),('Claws','Gwen','cat','m','1994-03-17','0000-00-00'),('Buffy','Harold','dog','f','1989-05-13','0000-00-00'),('Fang','Benny','dog','m','1990-08-27','0000-00-00'),('Bowser','Diane','dog','m','1979-08-31','1995-07-29'),('Chirpy','Gwen','bird','f','1998-09-11','0000-00-00'),('Whistler','Gwen','bird','','1997-12-09','0000-00-00'),('Slim','Benny','snake','m','1996-04-29','0000-00-00');
/*!40000 ALTER TABLE `pet` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `student`
--

DROP TABLE IF EXISTS `student`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `student` (
  `id` int(2) NOT NULL AUTO_INCREMENT,
  `name` varchar(50) NOT NULL DEFAULT '',
  `class` varchar(10) NOT NULL DEFAULT '',
  `mark` int(3) NOT NULL DEFAULT '0',
  UNIQUE KEY `id` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=36 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `student`
--

LOCK TABLES `student` WRITE;
/*!40000 ALTER TABLE `student` DISABLE KEYS */;
INSERT INTO `student` VALUES (1,'John Deo','Four',75),(2,'Max Ruin','Three',85),(3,'Arnold','Three',55),(4,'Krish Star','Four',60),(5,'John Mike','Four',60),(6,'Alex John','Four',55),(7,'My John Rob','Fifth',78),(8,'Asruid','Five',85),(9,'Tes Qry','Six',78),(10,'Big John','Four',55),(11,'Ronald','Six',89),(12,'Recky','Six',94),(13,'Kty','Seven',88),(14,'Bigy','Seven',88),(15,'Tade Row','Four',88),(16,'Gimmy','Four',88),(17,'Tumyu','Six',54),(18,'Honny','Five',75),(19,'Tinny','Nine',18),(20,'Jackly','Nine',65),(21,'Babby John','Four',69),(22,'Reggid','Seven',55),(23,'Herod','Eight',79),(24,'Tiddy Now','Seven',78),(25,'Giff Tow','Seven',88),(26,'Crelea','Seven',79),(27,'Big Nose','Three',81),(28,'Rojj Base','Seven',86),(29,'Tess Played','Seven',55),(30,'Reppy Red','Six',79),(31,'Marry Toeey','Four',88),(32,'Binn Rott','Seven',90),(33,'Kenn Rein','Six',96),(34,'Gain Toe','Seven',69),(35,'Rows Noump','Six',88);
/*!40000 ALTER TABLE `student` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2016-02-17 19:33:54
