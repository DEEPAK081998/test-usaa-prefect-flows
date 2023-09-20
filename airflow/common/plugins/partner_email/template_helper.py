from string import Template


class TemplateParser:
    extension: str = ''

    def __init__(self, template_filename: str, s3_bucket_name: str, parameters: dict) -> None:
        self.template_filename = template_filename
        self.s3_bucket_name = s3_bucket_name
        self.parameters = parameters

    def __str__(self) -> str:
        return self.load()

    def __repr__(self) -> str:
        return self.load()

    def _add_extension(self, template_filename: str) -> str:
        """Add file extension if needed."""

        assert template_filename
        template_filename = template_filename.lower()

        if not template_filename.endswith(self.extension):
            filename_with_extension = template_filename + self.extension
            return filename_with_extension
        else:
            return template_filename

    def _read(self, template_filename: str) -> str:
        """Load template from filename."""

        with open(template_filename) as f:
            template = f.read()
        return template

    def _parse(self, template: str, parameters: dict) -> str:
        """Load template."""

        print(f"template = {template}")
        print(f"parameters = {parameters}")

        try:
            parsed_template = Template(template).substitute(**parameters)
            parsed_template = Template(parsed_template).substitute(**parameters)
        except KeyError as missing_parameter:
            raise Exception(f"Missing template parameter: {missing_parameter}.")
        return parsed_template

    def _parse_old(self, template: str, parameters: dict) -> str:
        """Load template."""

        try:
            parsed_template = template.format(**parameters)
        except KeyError as missing_parameter:
            raise Exception(f"Missing template parameter: {missing_parameter}.")
        return parsed_template

    def load(self) -> str:
        """Read and parse template with specified parameters."""

        template_filename = self._add_extension(self.template_filename)
        template = self._read(template_filename)
        parsed_template = self._parse(template, self.parameters)
        return parsed_template


class HTMLParser(TemplateParser):
    extension = '.html'

    def __init__(self, template_filename: str, s3_bucket_name: str, parameters: dict) -> None:
        super().__init__(template_filename, s3_bucket_name, parameters)
