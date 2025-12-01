# dbt-watsonx-spark Changelog

- This file provides a full account of all changes to `dbt-watsonx-spark`.
- Changes are listed under the (pre)release in which they first appear. Subsequent releases include changes from previous releases.
- "Breaking changes" listed under a version may require action from end users or external maintainers when upgrading to that version.


## 0.1.2 – November 2025
**Added**
- Support avro file format

## 0.1.1 – October 2025
**Added**
- Support for incremental models in Watsonx.data  
- Improved error reporting for responses coming from Watsonx APIs with documntaion for some errors

**Changed**
- Enhanced adapter robustness when handling Watsonx query failures  
- Minor internal stability updates  



## 0.1.0 – September 2025
**Added**
- Support for dynamic versioning — compatible with whichever Watsonx API version is available  
- Initial support for the new architecture APIs  

**Changed**
- Aligned configuration model with Watsonx’s updated API endpoints  


## 0.0.9 – Internal Preview (June 2025)
**Added**
- Early prototype integrating dbt-core with Watsonx Spark through Query Server  
- Basic table creation and schema management  
- Limited incremental model support  


## 0.0.8 – Developer Preview (April 2025)
**Initial Implementation**
- Internal testing release for IBM Spark Team  
- Included connection profiles and environment validation  
- Base scaffolding for adapter structure  


## Previous Releases
For information on future minor or patch releases, see:
- [GitHub Releases](https://github.com/IBM/dbt-watsonx-spark/releases)