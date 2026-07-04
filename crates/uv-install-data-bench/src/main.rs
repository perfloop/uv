use std::fs;
use std::str::FromStr;
use std::time::Instant;
use uv_install_wheel::{Layout, LinkMode, InstallState};
use uv_pypi_types::Scheme;
use uv_distribution_filename::WheelFilename;

fn main() -> anyhow::Result<()> {
    let temp_dir = assert_fs::TempDir::new()?;
    let site_packages = temp_dir.path().join("site-packages");
    let bin_dir = temp_dir.path().join("bin");
    let data_dir = temp_dir.path().join("data");
    let include_dir = temp_dir.path().join("include");

    let scheme = Scheme {
        purelib: site_packages.clone(),
        platlib: site_packages.clone(),
        scripts: bin_dir.clone(),
        data: data_dir.clone(),
        include: include_dir.clone(),
    };

    let layout = Layout {
        sys_executable: temp_dir.path().join("python.exe"),
        python_version: (3, 10),
        os_name: "posix".to_string(),
        scheme,
    };

    fs::create_dir_all(&site_packages)?;
    fs::create_dir_all(&bin_dir)?;
    fs::create_dir_all(&data_dir)?;
    fs::create_dir_all(&include_dir)?;

    // Create a fake unzipped wheel directory
    let wheel_dir = temp_dir.path().join("wheel");
    fs::create_dir_all(&wheel_dir)?;

    let dist_info_dir = wheel_dir.join("foo-1.0.0.dist-info");
    fs::create_dir_all(&dist_info_dir)?;

    fs::write(dist_info_dir.join("WHEEL"), "Wheel-Version: 1.0\nRoot-Is-Purelib: true\n")?;
    fs::write(dist_info_dir.join("METADATA"), "Name: foo\nVersion: 1.0.0\n")?;

    let purelib_src = wheel_dir.join("foo-1.0.0.data").join("purelib").join("foo");
    fs::create_dir_all(&purelib_src)?;

    // Generate files
    for i in 0..1000 {
        let file_path = purelib_src.join(format!("file_{}.py", i));
        fs::write(&file_path, format!("print('file {}')", i))?;
    }

    // Build the RECORD file
    let mut record_contents = String::new();
    for i in 0..1000 {
        record_contents.push_str(&format!("foo-1.0.0.data/purelib/foo/file_{}.py,,\n", i));
    }
    for i in 0..4000 {
        record_contents.push_str(&format!("unrelated/file_{}.py,,\n", i));
    }
    record_contents.push_str("foo-1.0.0.dist-info/WHEEL,,\n");
    record_contents.push_str("foo-1.0.0.dist-info/METADATA,,\n");
    record_contents.push_str("foo-1.0.0.dist-info/RECORD,,\n");

    fs::write(dist_info_dir.join("RECORD"), record_contents)?;

    let filename = WheelFilename::from_str("foo-1.0.0-py3-none-any.whl").unwrap();
    let state = InstallState::default();

    // Measure the run
    let start = Instant::now();
    uv_install_wheel::install_wheel::<(), ()>(
        &layout,
        false,
        &wheel_dir,
        &filename,
        None,
        None,
        None,
        Some("uv"),
        false,
        LinkMode::Copy,
        &state,
    )?;
    let duration = start.elapsed();

    let ns_value = duration.as_nanos() as f64;
    println!("{{\"metric\":\"ns/op\",\"value\":{}}}", ns_value);

    Ok(())
}
