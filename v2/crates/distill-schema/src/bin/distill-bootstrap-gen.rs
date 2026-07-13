use std::path::PathBuf;

use distill_schema::bootstrap_builtins_v1::generated_bootstrap_control_spec_v1;
use distill_schema::bootstrap_gen_v1::{
    check_bootstrap_generation_v1, generate_bootstrap_table_artifact_v1,
    local_generator_input_bytes_v1,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    match (args.next(), args.next(), args.next()) {
        (Some(command), None, None) if command == "check" => {
            check_bootstrap_generation_v1()?;
            Ok(())
        }
        (Some(command), Some(path), None) if command == "emit-spec" => {
            let path = PathBuf::from(path);
            let bytes = generated_bootstrap_control_spec_v1()?.encode()?;
            std::fs::write(path, bytes)?;
            Ok(())
        }
        (Some(command), Some(path), None) if command == "emit-table" => {
            let artifact =
                generate_bootstrap_table_artifact_v1(&local_generator_input_bytes_v1()?)?;
            std::fs::write(PathBuf::from(path), artifact.bytes())?;
            println!("{}", artifact.resource_name());
            Ok(())
        }
        (Some(command), None, None) if command == "print-table-name" => {
            let artifact =
                generate_bootstrap_table_artifact_v1(&local_generator_input_bytes_v1()?)?;
            println!("{}", artifact.resource_name());
            Ok(())
        }
        _ => Err(concat!(
            "usage: distill-bootstrap-gen check\n",
            "       distill-bootstrap-gen emit-spec <control-spec-v1.dsb>\n",
            "       distill-bootstrap-gen emit-table <control-table-v1.dsca>\n",
            "       distill-bootstrap-gen print-table-name"
        )
        .into()),
    }
}
