use std::fs;
use std::time::Instant;
use uv_install_wheel::RecordEntry;
use uv_pypi_types::Scheme;
use uv_normalize::PackageName;
use uv_install_wheel::Layout;

fn main() -> anyhow::Result<()> {
    let temp_dir = assert_fs::TempDir::new()?;
    let site_packages = temp_dir.path().join("site-packages");
    let data_dir = site_packages.join("foo-1.0.0.data");

    let scheme = Scheme {
        purelib: site_packages.clone(),
        platlib: site_packages.clone(),
        scripts: temp_dir.path().join("bin"),
        data: temp_dir.path().join("data"),
        include: temp_dir.path().join("include"),
    };

    let layout = Layout {
        sys_executable: temp_dir.path().join("python.exe"),
        python_version: (3, 10),
        os_name: "posix".to_string(),
        scheme,
    };

    fs::create_dir_all(&site_packages)?;
    let purelib_src = data_dir.join("purelib").join("foo");
    fs::create_dir_all(&purelib_src)?;

    for i in 0..1000 {
        let file_path = purelib_src.join(format!("file_{}.py", i));
        fs::write(&file_path, format!("print('file {}')", i))?;
    }

    let mut record = Vec::with_capacity(5000);
    for i in 0..1000 {
        let file_path = purelib_src.join(format!("file_{}.py", i));
        let relative = file_path.strip_prefix(&site_packages)?;
        record.push(RecordEntry {
            path: relative.to_string_lossy().to_string(),
            hash: None,
            size: None,
        });
    }
    for i in 0..4000 {
        record.push(RecordEntry {
            path: format!("unrelated/file_{}.py", i),
            hash: None,
            size: None,
        });
    }

    let dist_name = PackageName::from_owned("foo".to_string())?;

    let start = Instant::now();
    uv_install_wheel::install_data(
        &layout,
        false,
        &site_packages,
        &data_dir,
        &dist_name,
        &[],
        &[],
        &mut record,
    )?;
    let duration = start.elapsed();

    let ns_value = duration.as_nanos() as f64;
    println!("{{\"metric\":\"ns/op\",\"value\":{}}}", ns_value);

    Ok(())
}
